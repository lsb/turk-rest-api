require './sql'
require './turk-credentials'
require 'sinatra'

def valid_question_type?(q)
  q.size == 1 && (q.has_key?('Radio') || q.has_key?('Text') || q.has_key?('ConstrainedText')) &&
    if q.has_key?('Radio')
      r = q['Radio']
      (r.has_key?('questionText') && r.has_key?('chooseOne')) &&
        r['chooseOne'].respond_to?(:to_ary) &&
        r['chooseOne'].all? {|c| c.respond_to? :to_str } &&
        r['questionText'].respond_to?(:to_str)
    elsif q.has_key?('Text')
      t = q['Text']
      (t.has_key?('questionText') && t.has_key?('defaultText')) &&
        t['questionText'].respond_to?(:to_str) &&
        t['defaultText'].respond_to?(:to_str)
    else
      ct = q['ConstrainedText']
      (ct.has_key?('questionText') && ct.has_key?('defaultText') && ct.has_key?('regex')) &&
        ct['questionText'].respond_to?(:to_str) &&
        ct['defaultText'].respond_to?(:to_str) &&
        ct['regex'].respond_to?(:to_str) &&
        (Regexp.new(ct['regex']) rescue false)
    end
end
def validate!(ps)
  instructions = ps['instructions']
  raw_question = ps['question']
  raw_knownAnswerQuestions = ps['knownAnswerQuestions']
  distinctUsers = ps['distinctUsers'] = (ps['distinctUsers'] || '1').to_i
  ps['addMinutes'] = addMinutes = (ps['addMinutes'] || '0').to_i
  ps['cost'] = cost = ps.has_key?("cost") ? ps['cost'].to_i : nil
  ps['uniqueAskId'] = uniqueAskId = ps['uniqueAskId'] || ''

  !instructions.nil? || halt(400, "need instructions")
  ps['question'] = question = JSON.parse(raw_question) rescue halt(400, "unparseable JSON")
  ps['knownAnswerQuestions'] = knownAnswerQuestions = raw_knownAnswerQuestions.nil? ? nil : (JSON.parse(raw_knownAnswerQuestions) rescue halt(400, "unparseable JSON"))
  (1..25).include?(distinctUsers) || halt(400, "distinctUsers must be between 1 and 40")
  (0..99).include?(addMinutes) || halt(400, "addMinutes must be between 0 and 99")
  (cost.nil? || (1..99).include?(cost)) || halt(400, "cost must be between 1 and 99")
  valid_question_type?(question) || halt(400, "invalid question")
  if !knownAnswerQuestions.nil?
    knownAnswerQuestions.has_key?('percentCorrect') || halt(400, "bad knownAnswerQuestions data type")
    (0..100).include?(knownAnswerQuestions['percentCorrect']) || halt(400, "bad known answer questions percent correct")
    aQ = knownAnswerQuestions['answeredQuestions']
    aQ.respond_to?(:to_ary) || halt(400, "bad known answer questions answered questions")
    aQ.all? {|aq| aq.has_key?('answer') && aq['answer'].respond_to?(:to_str) } || halt(400, "bad known answer questions answers")
    aQ.all? {|aq| aq.has_key?('question') && valid_question_type?(aq['question']) } || halt(400, "bad known answer questions question")
  end
end

put('/ask') {
  validate!(params)
  put_question_type_and_question_and_ask!(params['instructions'], params['question'], params['distinctUsers'], params['addMinutes'], params['cost'], params['knownAnswerQuestions'], params['uniqueAskId'], DB)
  ""
}

get('/ask') {
  validate!(params)
  answers = get_answers(params['instructions'], params['question'], params['distinctUsers'], params['addMinutes'], params['cost'], params['knownAnswerQuestions'], params['uniqueAskId'], DB)
  answers.nil? ? halt(404) : JSON.dump(answers)
}

post('/o') { ship_oldest_batch!(DB, Turk[:live_endpoint], Turk[:access], Turk[:secret_access]); "" }
post('/i') { consume_hits!(DB, Turk[:live_endpoint], Turk[:access], Turk[:secret_access]); "" }
get('/') { redirect "http://www.leebutterman.com/ask-human/" }
