require './amzn'
require 'sqlite3'
require 'json'

DB = SQLite3::Database.new("asks.db")
DB.results_as_hash = true
DB.type_translation = true

InsertQuestionType = "insert or ignore into question_types (id, instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters) values (:id, :instructions, :distinctUsers, :addMinutes, :cost, :knownAnswerQuestions, :overrideParameters)"
InsertQuestion = "insert or ignore into questions (id, question_type_id, question) values (:id, :question_type_id, :question)"
InsertAsk = "insert or ignore into asks (id, question_id, uniqueAskId) values (:id, :question_id, :uniqueAskId)"
SelectAsk = "select id from asks where id = :aid"
SelectAnswers = "select answer, worker_id, is_valid, d.hit_id from asks join answers on asks.id = answers.ask_id join assignments on assignments.id = answers.assignment_id left join disposed_hits d on assignments.hit_id = d.hit_id where asks.id = :aid"


SelectBatchOfOldestUnshippedAsk = "select is_valid, instructions, questions, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters from oldest_batch"

InsertShippedAsk = "insert into shipped_asks (hit_id, ask_id) values (:hit_id, (select id from asks a left join shipped_asks sa on a.id = sa.ask_id where sa.ask_id is null and question_id = :question_id order by a.created_at limit 1))"

SelectHitParameters = "select question, question_id, question_type_id, distinctUsers, knownAnswerQuestions, overrideParameters from question_types qt, questions q, asks a, shipped_asks sa where qt.id = q.question_type_id and q.id = a.question_id and a.id = sa.ask_id and sa.hit_id = :hit_id"
InsertAssignment = "insert or ignore into assignments (id, hit_id, worker_id, assignment, is_valid) values (:id, :hit_id, :worker_id, :assignment, :is_valid)"
InsertAnswer = "insert or ignore into answers (assignment_id, ask_id, answer) values (:assignment_id, (select ask_id from asks a join shipped_asks sa on sa.ask_id = a.id join assignments m using (hit_id) where question_id = :question_id and m.id = :assignment_id), :answer)"
InsertDisposedHit = "insert or ignore into disposed_hits (hit_id) values (:hit_id)"
SelectCorrectIncorrectCounts = "select sum(is_valid) as correct, sum(1-is_valid) as incorrect from assignments where hit_id = :hit_id"

def make_id_ask(qid, uniqueAskId)
  (qid + uniqueAskId.sha256).sha256
end

def put_question_type!(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters, db)
  db.execute(InsertQuestionType,
             'id' => make_id_question_type(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters),
             'instructions' => instructions, 'distinctUsers' => distinctUsers, 'addMinutes' => addMinutes, 'cost' => cost,
             'knownAnswerQuestions' => knownAnswerQuestions.nil? ? nil : JSON.dump(knownAnswerQuestions),
             'overrideParameters' => overrideParameters)
end

def put_question!(instructions, question, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters, db)
  qtid = make_id_question_type(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters)
  qid = make_id_questions(qtid, [question]).keys.first
  db.execute(InsertQuestion, 'id' => qid, 'question_type_id' => qtid, "question" => JSON.dump(question))
end

def put_ask!(instructions, question, distinctUsers, addMinutes, cost, knownAnswerQuestions, uniqueAskId, overrideParameters, db)
  qtid = make_id_question_type(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters)
  qid = make_id_questions(qtid, [question]).keys.first
  aid = make_id_ask(qid, uniqueAskId)
  db.execute(InsertAsk, 'id' => aid, 'question_id' => qid, 'uniqueAskId' => uniqueAskId)
end

def put_question_type_and_question_and_ask!(instructions, question, distinctUsers, addMinutes, cost, knownAnswerQuestions, uniqueAskId, overrideParameters, db)
  db.transaction { |txn|
    put_question_type!(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters, txn)
    put_question!(instructions, question, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters, txn)
    put_ask!(instructions, question, distinctUsers, addMinutes, cost, knownAnswerQuestions, uniqueAskId, overrideParameters, txn)
  }
end

def get_answers(instructions, question, distinctUsers, addMinutes, cost, knownAnswerQuestions, uniqueAskId, overrideParameters, db)
  qtid = make_id_question_type(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions, overrideParameters)
  qid = make_id_questions(qtid, [question]).keys.first
  aid = make_id_ask(qid, uniqueAskId)
  maybe_ask = db.execute(SelectAsk, "aid" => aid)[0]
  return nil if maybe_ask.nil?
  answers = db.execute(SelectAnswers, "aid" => aid)
  no_more = answers.size > 0 && !answers[0].fetch("hit_id").nil?
  [no_more, answers.map {|a| a["is_valid"].zero? ? {"Fail" => {"value" => a["answer"], "worker" => a["worker_id"]}} : {"Pass" => {"value" => a["answer"], "worker" => a["worker_id"]}} }]
end


def get_oldest_batch(db)
  b = db.execute(SelectBatchOfOldestUnshippedAsk)[0]
  return nil if b.fetch('is_valid').zero?
  b.delete('is_valid')
  b['questions'] = JSON.parse(b.fetch('questions'))
  b['knownAnswerQuestions'] = JSON.parse(b['knownAnswerQuestions']) if !b.fetch('knownAnswerQuestions').nil?
  b
end

def ship_oldest_batch!(db, endpoint, access_key, secret_access_key, maybe_queue = nil)
  b = get_oldest_batch(db)
  return if b.nil?
  hits_idquestions = ship_all!(b.fetch('instructions'), b.fetch('questions'), b.fetch('distinctUsers'), b.fetch('addMinutes'), b.fetch('cost'), b.fetch('knownAnswerQuestions'), b.fetch('overrideParameters'), endpoint, access_key, secret_access_key, maybe_queue)
  hits_idquestions.each {|hit_id, idquestions|
    idquestions.keys.each {|question_id|
      db.execute(InsertShippedAsk, "hit_id" => hit_id, "question_id" => question_id)
    }
  }
  nil
end

def consume_assignments!(db, queue_endpoint, queue_access, queue_secret, turk_endpoint, turk_access, turk_secret)
  notifications = receive_assignment_notifications!(queue_endpoint, queue_access, queue_secret)
  return if notifications.length.zero?
  p(notifications.map {|n| JSON.parse(n.body) })
  assignment_ids_hit_ids = notifications.map {|n| JSON.parse(n.body)["Events"].map {|e| [e["AssignmentId"], e["HITId"]] } }.inject(&:+)
  assignment_ids_hit_ids.each {|assignment_id, hit_id|
    hit_params = db.execute(SelectHitParameters, "hit_id" => hit_id)
    qtypecount = hit_params.map {|h| h.fetch("question_type_id") }.uniq.size
    if qtypecount != 1
      warn "disregarding hit #{hit_id}, number of question type = #{qtypecount}"
      warn " '-> disregarding assignment #{assignment_id}"
      next
    end
    distinctUsers = hit_params.first.fetch('distinctUsers')
    knownAnswerQuestions = hit_params.first.fetch("knownAnswerQuestions")
    knownAnswerQuestions = JSON.parse(knownAnswerQuestions) if !knownAnswerQuestions.nil?
    id_questions = hit_params.inject({}) {|h, param| h[param.fetch("question_id")] = JSON.parse(param.fetch('question')) ; h }
    p id_questions
    a = assignment!(assignment_id, knownAnswerQuestions, turk_endpoint, turk_access, turk_secret)
    db.execute(InsertAssignment, "id" => a.fetch("id"), "hit_id" => hit_id, "worker_id" => a.fetch("worker_id"), "assignment" => JSON.dump(a.fetch("assignment")), "is_valid" => a.fetch("valid?") ? 1 : 0)
    a.fetch("assignment").each {|question_id, answer|
      db.execute(InsertAnswer, "assignment_id" => a.fetch("id"), "question_id" => question_id, "answer" => answer.to_json)
    }
    correct_incorrect_counts = db.execute(SelectCorrectIncorrectCounts, "hit_id" => hit_id)[0]
    correct_count = correct_incorrect_counts.fetch("correct")
    incorrect_count = correct_incorrect_counts.fetch("incorrect")
    extend_hit!(hit_id, 1, turk_endpoint, turk_access, turk_secret) if !a["valid?"] && incorrect_count < distinctUsers
    (dispose_hit!(hit_id, turk_endpoint, turk_access, turk_secret) if correct_count == distinctUsers || incorrect_count > distinctUsers) rescue nil
    db.execute(InsertDisposedHit, "hit_id" => hit_id)
  }
  discard_assignment_notifications!(notifications, queue_endpoint, queue_access, queue_secret)
end
