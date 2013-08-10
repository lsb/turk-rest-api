require 'rest-client'
require 'openssl'
require 'date'
require 'base64'
require 'rexml/document'
require 'builder'
require 'htmlentities'
require 'aws-sdk'

class String
  def sha256
    Base64.urlsafe_encode64(OpenSSL::Digest::SHA256.digest(self))
  end
end

def add_signature(h, access_key, secret_access_key)
  timestamp = DateTime.now.xmlschema
  service = "AWSMechanicalTurkRequester"
  operation = h.fetch("Operation")
  signature = Base64.strict_encode64(OpenSSL::HMAC.digest("sha1", secret_access_key, service+operation+timestamp))
  h.merge({"Timestamp" => timestamp, "Service" => service, "AWSAccessKeyId" => access_key, "Signature" => signature})
end

def turk!(h, endpoint, access_key, secret_access_key)
  RestClient.post(endpoint, add_signature(h, access_key, secret_access_key))
end

def valid_response?(x)
  x.root.get_elements("//IsValid").map(&:text).first == "True"
end

def valid_xml!(r)
  x = REXML::Document.new(r)
  raise RuntimeError, r unless valid_response?(x)
  x
end

def account_balance!(endpoint, access_key, secret_access_key)
  ab = turk!({"Operation" => "GetAccountBalance"}, endpoint, access_key, secret_access_key)
  x = valid_xml!(ab)
  x.elements['/GetAccountBalanceResponse/GetAccountBalanceResult/AvailableBalance/Amount/text()'].to_s.to_f
end

def block!(worker_id, reason, endpoint, access_key, secret_access_key)
  bw = turk!({"Operation" => "BlockWorker", "WorkerId" => worker_id, "Reason" => reason}, endpoint, access_key, secret_access_key)
  x = valid_xml!(bw)
  true
end

def bonus!(worker_id, assignment_id, amt, reason, endpoint, access_key, secret_access_key)
  gb = turk!({"Operation" => "GrantBonus", "WorkerId" => worker_id, "AssignmentId" => assignment_id, "BonusAmount.1.Amount" => amt.round(2), "BonusAmount.1.CurrencyCode" => "USD", "Reason" => reason})
  x = valid_xml!(gb)
  true
end

def subscribe_hit_type_assignments_to_queue!(hit_type_id, queue, endpoint, access_key, secret_access_key)
  s = turk!({"Operation" => "SetHITTypeNotification", "HITTypeId" => hit_type_id, "Notification.1.Transport" => "SQS", "Notification.1.Version" => "2006-05-05", "Notification.1.Destination" => queue, "Notification.1.EventType" => "AssignmentSubmitted"}, endpoint, access_key, secret_access_key)
  x = valid_xml!(s)
  true
end

def receive_assignment_notifications!(endpoint, access_key, secret_access_key)
  AWS.config({:access_key_id => access_key, :secret_access_key => secret_access_key})
  AWS::SQS::Queue.new(endpoint).receive_message(:limit => 10)
end

def discard_assignment_notifications!(notifications, endpoint, access_key, secret_access_key)
  AWS.config({:access_key_id => access_key, :secret_access_key => secret_access_key})
  AWS::SQS::Queue.new(endpoint).batch_delete(notifications)
end

def ship_hit!(h, endpoint, access_key, secret_access_key, maybe_queue = nil)
  sh = turk!(h, endpoint, access_key, secret_access_key)
  x = valid_xml!(sh)
  hit_type_id = x.elements['/CreateHITResponse/HIT/HITTypeId/text()'].to_s
  subscribe_hit_type_assignments_to_queue!(hit_type_id, maybe_queue, endpoint, access_key, secret_access_key) unless maybe_queue.nil?
  hit_id = x.elements['/CreateHITResponse/HIT/HITId/text()'].to_s
  hit_id
end

def extend_hit!(hit_id, increment, endpoint, access_key, secret_access_key)
  eh = turk!({"Operation" => "ExtendHIT", "HITId" => hit_id, "MaxAssignmentsIncrement" => increment}, endpoint, access_key, secret_access_key)
  x = valid_xml!(eh)
  true
end

def dispose_hit!(hit_id, endpoint, access_key, secret_access_key)
  dh = turk!({"Operation" => "DisposeHIT", "HITId" => hit_id}, endpoint, access_key, secret_access_key)
  x = valid_xml!(dh)
  true
end

def reviewable_hit_ids!(endpoint, access_key, secret_access_key)
  rhids = turk!({"Operation" => "GetReviewableHITs"}, endpoint, access_key, secret_access_key)
  x = valid_xml!(rhids)
  x.root.get_elements("//HITId").map(&:text)
end

def hit_assignments!(hit_id, endpoint, access_key, secret_access_key)
  # get a max of 100 different workers at a time
  ha = turk!({"Operation" => "GetAssignmentsForHIT", "HITId" => hit_id, "PageSize" => 100}, endpoint, access_key, secret_access_key)
  x = valid_xml!(ha)
  x.root.get_elements("//Assignment").map {|a|
    assignment_id = a.get_elements("AssignmentId").first.text
    worker_id = a.get_elements("WorkerId").first.text
    answer_xml = REXML::Document.new(a.get_elements("Answer").first.text).root
    answer_hash = {}
    answer_xml.get_elements("Answer").each {|a|
      qi = a.get_elements("QuestionIdentifier").first.text
      maybe_freetext = a.get_elements("FreeText").first
      maybe_selection = a.get_elements("SelectionIdentifier").first
      answer = (maybe_freetext || maybe_selection).text
      answer_hash[qi] = answer
    }
    {"id" => assignment_id, "worker_id" => worker_id, "assignment" => answer_hash}
  }
end

def assignment!(assignment_id, knownAnswerQuestions, endpoint, access_key, secret_access_key)
  t = turk!({"Operation" => "GetAssignment", "AssignmentId" => assignment_id}, endpoint, access_key, secret_access_key)
  x = valid_xml!(t)
  a = x.root.elements["/GetAssignmentResponse/GetAssignmentResult/Assignment"]
  hit_id = a.elements["HITId"].text
  worker_id = a.elements["WorkerId"].text
  answer_xml = REXML::Document.new(a.elements["Answer"].text).root
  answer_hash = {}
  answer_xml.get_elements("Answer").each {|answer|
    qi = answer.elements["QuestionIdentifier"].text
    maybe_freetext = answer.elements["FreeText"]
    maybe_selection = answer.elements["SelectionIdentifier"]
    answer_text = (maybe_freetext || maybe_selection).text
    p "ehlol"
    p({qi => answer_text })
    p "hello"
    answer_hash[qi] = answer_text
  }
  is_valid = knownAnswerQuestions.nil? || gs_percent_correct(answer_hash) >= knownAnswerQuestions.fetch("percentCorrect")
  {"id" => assignment_id, "hit_id" => hit_id, "worker_id" => worker_id, "assignment" => answer_hash, "valid?" => is_valid}
end

def maybe_hit_assignments!(hit_id, distinctUsers, knownAnswerQuestions, endpoint, access_key, secret_access_key)
  some_assignments = hit_assignments!(hit_id, endpoint, access_key, secret_access_key)
  answer_hashes = some_assignments.map {|a| a.fetch("assignment") }
  percentCorrect = knownAnswerQuestions.nil? ? nil : knownAnswerQuestions.fetch("percentCorrect")
  more_count = assignments_needed(answer_hashes, distinctUsers, percentCorrect)
  warn "fuzzy math for #{hit_id}" if more_count < 0
  if more_count > 0
    extend_hit!(hit_id, more_count, endpoint, access_key, secret_access_key)
    nil
  else
    Thread.new { sleep 120 ; dispose_hit!(hit_id, endpoint, access_key, secret_access_key) }
    some_assignments.map {|a| a.merge({"valid?" => percentCorrect.nil? || gs_percent_correct(a.fetch('assignment')) >= percentCorrect}) }
  end
end

def batch_into_hits(instructions, questions, distinctUsers, addMinutes, cost, knownAnswerQuestions)
  raise ZeroDivisionError if questions.length.zero?
  id_questions = make_id_questions(make_id_question_type(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions), questions)
  shuffled_questions = questions #id_questions.sort_by {|i,q| i}.map {|i,q| q }
  max_possible_question_form_length = 128 * 1024
  max_desired_questions = 15
  max_desired_duration = 3600
  h = make_hit(instructions, shuffled_questions, distinctUsers, addMinutes, cost, knownAnswerQuestions)
  batch_too_full = h.fetch("Question").length > max_possible_question_form_length || h.fetch("AssignmentDurationInSeconds") > max_desired_duration || questions.length > max_desired_questions
  evens_odds = shuffled_questions.each_with_index.partition {|_,i| i.even? }.map {|part| part.map {|e,_| e } }
  batch_too_full ? evens_odds.map {|question_batch| batch_into_hits(instructions, question_batch, distinctUsers, addMinutes, cost, knownAnswerQuestions)}.inject(&:merge) : {h => id_questions}
end

def ship_all!(instructions, questions, distinctUsers, addMinutes, cost, knownAnswerQuestions, endpoint, access_key, secret_access_key, maybe_queue = nil)
  hits_idquestions = batch_into_hits(instructions, questions, distinctUsers, addMinutes, cost, knownAnswerQuestions)
  hits_idquestions.map {|hit,id_questions| {ship_hit!(hit, endpoint, access_key, secret_access_key, maybe_queue) => id_questions} }.inject(&:merge)
end

def gs_percent_correct(answer_hash)
  golds = answer_hash.find_all {|qid, a| qid.start_with?("g") }
  good_golds = golds.find_all {|qid, a|
    gid, m = *qid.split('~')
    match = JSON.parse(Base64.urlsafe_decode64(m))
    exact = match.has_key?('Exact')
    string = match.fetch(exact ? 'Exact' : 'Inexact')
    exact ? a == string : !!a.match(Regexp.new(string))
  }
  (good_golds.size * 100.0 / golds.size).round(2)
end

def assignments_needed(answer_hashes, distinctUsers, percentCorrect)
  if percentCorrect.nil?
    distinctUsers - answer_hashes.size
  else
    correct_count = answer_hashes.find_all {|ah| gs_percent_correct(ah) >= percentCorrect }.size
    incorrect_count = answer_hashes.size - correct_count
    [distinctUsers + 1 - incorrect_count, distinctUsers - correct_count].min
  end
end

def plain_text(text)
  Builder::XmlMarkup.new.FormattedContent {|b|
    cdata = text.split("\n").map {|line|
      HTMLEntities.new.encode(line, :hexadecimal)
    }.join("<br>")
    b.cdata!(cdata)
  }
end

def plain_image(url)
  Builder::XmlMarkup.new.Binary {|b|
    b.MimeType { b.Type("image") }
    b.DataURL(url)
    b.AltText(url)
  }
end

def escaped_text(text)
  b = Builder::XmlMarkup.new
  text.scan(/\\image\\(http[^\\]+)\\|((?:(?!\\image\\).)+)/) {|maybe_image, maybe_literals|
    maybe_literals ? b << plain_text(maybe_literals) : b << plain_image(maybe_image)
  }
  b.target!    
end

def generate_radio_answer(answers)
  Builder::XmlMarkup.new.SelectionAnswer {|b|
    b.StyleSuggestion("radiobutton")
    b.Selections {
      answers.each {|a|
        b.Selection {
          b.SelectionIdentifier(a)
          b << escaped_text(a)
        }
      }
    }
  }
end

def generate_constrained_text_answer(default, regex)
  Builder::XmlMarkup.new.FreeTextAnswer {|b|
    b.Constraints {
      b.AnswerFormatRegex(:regex => regex, :flags => "i")
    }
    b.DefaultText(default)
  }
end

def generate_text_answer(default)
  Builder::XmlMarkup.new.FreeTextAnswer {|b|
    b.DefaultText(default)
  }
end

def generate_question_form(instructions, id_questions)
  Builder::XmlMarkup.new.QuestionForm(:xmlns => "http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2005-10-01/QuestionForm.xsd") {|b|
    b.Overview { b << escaped_text(instructions) }
    id_questions.each {|uid, q|
      b.Question {
        b.QuestionIdentifier(uid)
        b.IsRequired("true")
        b.QuestionContent {
          b << escaped_text(if q.has_key?("Radio")
                              q['Radio'].fetch("questionText")
                            elsif q.has_key?("ConstrainedText")
                              q['ConstrainedText'].fetch("questionText")
                            else
                              q['Text'].fetch('questionText')
                            end
                            )
        }
        b.AnswerSpecification {
          b << (if q.has_key?('Radio')
                  generate_radio_answer(q['Radio'].fetch('chooseOne'))
                elsif q.has_key?('ConstrainedText')
                  generate_constrained_text_answer(q['ConstrainedText'].fetch('defaultText'), q['ConstrainedText'].fetch('regex'))
                else
                  generate_text_answer(q['Text'].fetch('defaultText'))
                end)
        }
      }
    }
  }
end

def question_text(q)
  fields = if q.has_key?('Radio')
             [q['Radio'].fetch('questionText'), q['Radio'].fetch('chooseOne').join(" ")]
           elsif q.has_key?('Text')
             [q['Text'].fetch('questionText'), q['Text'].fetch('defaultText')]
           else
             [q['ConstrainedText'].fetch('questionText'), q['ConstrainedText'].fetch('defaultText'), q['ConstrainedText'].fetch('regex')]
           end
  fields.join(" ")
end

def make_id_question_type(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions)
  slug = [instructions, distinctUsers.to_s, addMinutes.to_s, (cost.nil? ? 0 : cost).to_s].map(&:sha256).join
  knownAnswerSlug = knownAnswerQuestions.nil? ? "" : [knownAnswerQuestions.fetch("percentCorrect").to_s.sha256,
                                                      knownAnswerQuestions.fetch("answeredQuestions").map {|aq|
                                                        m = aq.fetch("match")
                                                        k = m.has_key?("Exact") ? "Exact" : "Inexact"
                                                        [k, m.fetch(k), question_text(aq.fetch("question"))].map(&:sha256).join
                                                      }.join].join
  (slug + knownAnswerSlug).sha256
end

def make_id_questions(qtid, questions)
  questions.inject({}) {|h, q| h['q' + (qtid + question_text(q).sha256).sha256] = q ; h }
end

def gs_make_id_questions(knownAnswerQuestions)
  h = {}
  knownAnswerQuestions.fetch("answeredQuestions").each {|aq|
    m = aq.fetch("match")
    q = aq.fetch("question")
    h['g' + question_text(q).sha256 + '~' + Base64.urlsafe_encode64(JSON.dump(m))] = q
  }
  h
end

def make_hit(instructions, questions, distinctUsers, addMinutes, cost, knownAnswerQuestions)
  gs = knownAnswerQuestions.nil? ? {} : gs_make_id_questions(knownAnswerQuestions)
  qs = make_id_questions(make_id_question_type(instructions, distinctUsers, addMinutes, cost, knownAnswerQuestions), questions)
  question_batch = gs.merge(qs).sort_by {|i, q| i.sha256 }
  question_form = generate_question_form(instructions, question_batch)
  all_questions = gs.values + qs.values
  reward = cost.nil? ? hit_reward(all_questions) : (all_questions.size * cost)./(5.0).ceil./(20.0)
  duration = time_allotment(all_questions, instructions, addMinutes)
  title = instructions.lstrip[/[^\n]+/][0,127]
  { "Operation" => "CreateHIT",
    "Title" => title,
    "Description" => title,
    "Question" => question_form,
    "Reward.1.CurrencyCode" => "USD",
    "Reward.1.Amount" => reward,
    "QualificationRequirement.1.QualificationTypeId" => "000000000000000000L0",
    "QualificationRequirement.1.Comparator" => "GreaterThan",
    "QualificationRequirement.1.IntegerValue" => "99",
    "QualificationRequirement.1.RequiredToPreview" => "false",
    "QualificationRequirement.2.QualificationTypeId" => "00000000000000000040",
    "QualificationRequirement.2.Comparator" => "GreaterThan",
    "QualificationRequirement.2.IntegerValue" => "1000",
    "AssignmentDurationInSeconds" => duration,
    "LifetimeInSeconds" => (3600 * 6),
    "MaxAssignments" => distinctUsers,
    "AutoApprovalDelayInSeconds" => 0}#,  TODO FIX NOT USING UNIQUEASKID
    #"UniqueRequestToken" => (gs.keys+qs.keys).map(&:sha256).join.sha256 }
end

def seconds_to_read(questions)
  words_per_second = 3.75 # = 225 wpm (brisk)
  joined_words = questions.map {|q| question_text(q) }.join(" ")
  word_count = joined_words.scan(/\w+/).size.to_f
  word_count / words_per_second
end

def time_allotment(questions, instructions, addMinutes)
  min_seconds = 60 # as per amzn
  seconds = seconds_to_read(questions + [{'Text' => {'questionText' => instructions, 'defaultText' => ''}}]).to_i * 15
  total_seconds = seconds + 60 * addMinutes
  quantized_seconds = total_seconds./(1800.0).ceil.*(1800)
  [quantized_seconds, min_seconds].max
end

def hit_reward(questions)
  min_reward = 0.01
  reading_seconds = seconds_to_read(questions)
  seconds_per_hour = 3600.0
  mturk_hourly_living_wage = 4.00
  reward = (reading_seconds / seconds_per_hour) * mturk_hourly_living_wage
  quantized_reward = (2 ** (Math.log2(reward*100).round)).round / 100.0
  [quantized_reward, min_reward].max
end
