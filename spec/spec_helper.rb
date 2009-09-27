$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'spec'
require 'appengine-sdk'
AppEngine::SDK.load_apiproxy
require 'appengine-apis/testing'
AppEngine::Testing.install_test_env

Spec::Runner.configure do |config|
  # config.include(ProtoMethods)  
end
