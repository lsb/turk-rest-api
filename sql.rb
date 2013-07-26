require './amzn'
require 'sqlite3'
require 'json'

DB = SQLite3::Database.new("asks.db")
DB.results_as_hash = true
DB.type_translation = true

InsertQuestionType = "insert or ignore into question_types (id, instructions, distinctUsers, addMinutes, addCents, knownAnswerQuestions) values (:id, :instructions, :distinctUsers, :addMinutes, :addCents, :knownAnswerQuestions)"
InsertQuestion = "insert or ignore into questions (id, question_type_id, question) values (:id, :question_type_id, :question)"
InsertAsk = "insert or ignore into asks (id, question_id, uniqueAskId) values (:id, :question_id, :uniqueAskId)"
SelectAnswers = "select (case when exists (select * from assignments join shipped_asks using (hit_id) where ask_id = :aid) then '[' || coalesce(group_concat(answer),'') || ']' else null end) as answer_list from answers where ask_id = :aid"


SelectBatchOfOldestUnshippedAsk = "select is_valid, instructions, questions, distinctUsers, addMinutes, addCents, knownAnswerQuestions from oldest_batch"

InsertShippedAsk = "insert into shipped_asks (hit_id, ask_id) values (:hit_id, (select id from asks a left join shipped_asks sa on a.id = sa.ask_id where sa.ask_id is null and question_id = :question_id order by a.created_at limit 1))"

SelectHitParameters = "select question, question_id, question_type_id, distinctUsers, knownAnswerQuestions from question_types qt, questions q, asks a, shipped_asks sa where qt.id = q.question_type_id and q.id = a.question_id and a.id = sa.ask_id and sa.hit_id = :hit_id"
InsertAssignment = "insert or ignore into assignments (id, hit_id, worker_id, assignment) values (:id, :hit_id, :worker_id, :assignment)"
InsertAnswer = "insert or ignore into answers (assignment_id, ask_id, answer) values (:assignment_id, (select ask_id from asks a join shipped_asks sa on sa.ask_id = a.id join assignments m using (hit_id) where question_id = :question_id and m.id = :assignment_id), :answer)"


def make_id_ask(qid, uniqueAskId)
  (qid + uniqueAskId.sha256).sha256
end

def put_question_type!(instructions, distinctUsers, addMinutes, addCents, knownAnswerQuestions, db)
  db.execute(InsertQuestionType,
             'id' => make_id_question_type(instructions, distinctUsers, addMinutes, addCents, knownAnswerQuestions),
             'instructions' => instructions, 'distinctUsers' => distinctUsers, 'addMinutes' => addMinutes, 'addCents' => addCents,
             'knownAnswerQuestions' => knownAnswerQuestions.nil? ? nil : JSON.dump(knownAnswerQuestions) )
end

def put_question!(instructions, question, distinctUsers, addMinutes, addCents, knownAnswerQuestions, db)
  qtid = make_id_question_type(instructions, distinctUsers, addMinutes, addCents, knownAnswerQuestions)
  qid = make_id_questions(qtid, [question]).keys.first
  db.execute(InsertQuestion, 'id' => qid, 'question_type_id' => qtid, "question" => JSON.dump(question))
end

def put_ask!(instructions, question, distinctUsers, addMinutes, addCents, knownAnswerQuestions, uniqueAskId, db)
  qtid = make_id_question_type(instructions, distinctUsers, addMinutes, addCents, knownAnswerQuestions)
  qid = make_id_questions(qtid, [question]).keys.first
  aid = make_id_ask(qid, uniqueAskId)
  db.execute(InsertAsk, 'id' => aid, 'question_id' => qid, 'uniqueAskId' => uniqueAskId)
end

def put_question_type_and_question_and_ask!(instructions, question, distinctUsers, addMinutes, addCents, knownAnswerQuestions, uniqueAskId, db)
  db.transaction { |txn|
    put_question_type!(instructions, distinctUsers, addMinutes, addCents, knownAnswerQuestions, txn)
    put_question!(instructions, question, distinctUsers, addMinutes, addCents, knownAnswerQuestions, txn)
    put_ask!(instructions, question, distinctUsers, addMinutes, addCents, knownAnswerQuestions, uniqueAskId, txn)
  }
end

def get_answers(instructions, question, distinctUsers, addMinutes, addCents, knownAnswerQuestions, uniqueAskId, db)
  qtid = make_id_question_type(instructions, distinctUsers, addMinutes, addCents, knownAnswerQuestions)
  qid = make_id_questions(qtid, [question]).keys.first
  aid = make_id_ask(qid, uniqueAskId)
  maybe_answer_list = db.execute(SelectAnswers, "aid" => aid)[0]['answer_list']
  maybe_answer_list.nil? ? nil : JSON.parse(maybe_answer_list)
end


def get_oldest_batch(db)
  b = db.execute(SelectBatchOfOldestUnshippedAsk)[0]
  return nil if b.fetch('is_valid').zero?
  b.delete('is_valid')
  b['questions'] = JSON.parse(b.fetch('questions'))
  b['knownAnswerQuestions'] = JSON.parse(b['knownAnswerQuestions']) if !b.fetch('knownAnswerQuestions').nil?
  b
end

def ship_oldest_batch!(db, endpoint, access_key, secret_access_key)
  b = get_oldest_batch(db)
  return if b.nil?
  hits_idquestions = ship_all!(b.fetch('instructions'), b.fetch('questions'), b.fetch('distinctUsers'), b.fetch('addMinutes'), b.fetch('addCents'), b.fetch('knownAnswerQuestions'), endpoint, access_key, secret_access_key)
  hits_idquestions.each {|hit_id, idquestions|
    idquestions.keys.each {|question_id|
      db.execute(InsertShippedAsk, "hit_id" => hit_id, "question_id" => question_id)
    }
  }
  nil
end


def consume_hits!(db, endpoint, access_key, secret_access_key)
  reviewable_hit_ids!(endpoint, access_key, secret_access_key).each {|hit_id|
    hit_params = db.execute(SelectHitParameters, "hit_id" => hit_id)
    qtypecount = hit_params.map {|h| h.fetch('question_type_id') }.uniq.size
    if qtypecount != 1
      warn "disregarding hit #{hit_id}, number of question_types = #{qtypecount}"
      warn " '-> disregarding assignments #{maybe_hit_assignments!(hit_id, 0, nil, endpoint, access_key, secret_access_key)}"
    else
      distinctUsers = hit_params.first.fetch('distinctUsers')
      knownAnswerQuestions = hit_params.first.fetch("knownAnswerQuestions")
      knownAnswerQuestions = JSON.parse(knownAnswerQuestions) if !knownAnswerQuestions.nil?
      id_questions = hit_params.inject({}) {|h, param| h[param.fetch('question_id')] = JSON.parse(param.fetch('question')); h }
      p id_questions
      maybe_assignments = maybe_hit_assignments!(hit_id, distinctUsers, knownAnswerQuestions, endpoint, access_key, secret_access_key)
      p maybe_assignments
      if !maybe_assignments.nil?
        maybe_assignments.each {|a|
          STDERR.puts "id => #{a['id']} hit_id => #{hit_id} worker_id => #{a['worker_id']} assignment => #{a['assignment'].inspect}"
          db.execute(InsertAssignment, "id" => a.fetch("id"), "hit_id" => hit_id, "worker_id" => a.fetch("worker_id"), "assignment" => JSON.dump(a.fetch("assignment")))
          if a.fetch("valid?")
            a.fetch("assignment").each {|question_id, answer|
              question = id_questions.fetch(question_id)
              stored_answer = question.has_key?("Radio") ? question['Radio'].fetch('chooseOne').find {|raw_answer| raw_answer.sha256 == answer } : answer
              STDERR.puts "assignment_id => #{a['id']} question_id => #{question_id} answer => #{stored_answer.inspect} raw_answer => #{answer}"
              db.execute(InsertAnswer, "assignment_id" => a.fetch("id"), "question_id" => question_id, "answer" => stored_answer.to_json)
            }
          end
        }
      end
    end
  }
end
