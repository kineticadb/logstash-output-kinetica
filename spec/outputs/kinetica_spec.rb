require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/kinetica"
require "logstash/codecs/plain"
require "logstash/logging"

class LogStash::Outputs::Kinetica
  attr_writer :agent
  attr_reader :request_tokens
	LogStash::Logging::Logger.reconfigure("./log4j2.properties")
end

describe LogStash::Outputs::Kinetica do
  let(:event) {
    LogStash::Event.new({"message" => "1\\23"})
  }
  let(:url) { "http://192.168.56.101:9191/insert/records/frompayload" }
  let(:method) { :post }

  shared_examples("basic tests") do
		let(:basic_tests_config) { {"url" => url, "pool_max" => 1, "user" => "admin", "password" => "***",\
			"table_name" => "jkr_test", "options" => {"text_has_header"=>"false", "error_handling"=>"abort"} } }
		subject { LogStash::Outputs::Kinetica.new(basic_tests_config) }

		let(:expected_method) { method.clone.to_sym }
		let(:client) { subject.client }

		before do
			subject.register
			allow(client).to receive(:send).
												 with(expected_method, url, anything).
												 and_call_original
			allow(subject).to receive(:log_failure).with(any_args)
			allow(subject).to receive(:log_retryable_response).with(any_args)
		end


	context "performing a request" do
			describe "invoking the request" do
				before do
					subject.multi_receive([event])
				end

				it "should execute the request" do
					expect(client).to have_received(:send).
															with(expected_method, url, anything)
				end
			end

			context "with passing requests" do
				before do
					subject.multi_receive([event])
				end

				it "should not log a failure" do
					expect(subject).not_to have_received(:log_failure).with(any_args)
				end
			end
    end
  end

	context "basic test" do
		include_examples("basic tests")
	end
end
