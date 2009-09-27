require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
require 'appengine-pstore'
require 'pp'

describe AppEngine::PStore do
  # initializing datastore in memory each time
  before :each do
    AppEngine::Testing.install_test_datastore
    @db = AppEngine::PStore.new('test.pstore')
  end

  after :each do
    #it should close transactions
    AppEngine::Datastore.active_transactions.to_a.should == []
  end

  it 'should support get/put' do
    @db.transaction do |db|
      db['var1'] = "hello"
      db['var2'] = "world"
      db['var1'].should == "hello"
      db['var2'].should == "world"
    end
    @db.transaction do |db|
      db['var1'].should == "hello"
      db['var2'].should == "world"
    end
  end

  it 'should support symbol key' do
    @db.transaction do |db|
      db[:key] = "hello"
      db['key'] = "world"
    end
    @db.transaction do |db|
      db[:key].should == "hello"
      db['key'].should == "world"
    end
  end

  it 'should return nil' do
    @db.transaction do |db|
      db['undefined'].should == nil
      db.delete('undefined').should == nil
    end
  end

  it 'should support multiple database' do
    db1 = AppEngine::PStore.new('test1.pstore')
    db2 = AppEngine::PStore.new('test2.pstore')
    db1.transaction do |db1|
      db2.transaction do |db2|
        db1[:key] = "hello"
        db2[:key] = "world"
        db1[:key].should == "hello"
        db2[:key].should == "world"
      end
    end

    db1.transaction do |db1|
      db2.transaction do |db2|
        db1[:key].should == "hello"
        db2[:key].should == "world"
      end
    end
  end

  it 'should support delete' do
    @db.transaction do |db|
      db[:key] = 'hello'
    end
    @db.transaction do |db|
      value = db.delete(:key)
      value.should == 'hello'
      db[:key].should == nil
    end
    @db.transaction do |db|
      db[:key].should == nil
    end
  end

  it 'should suppert roots' do
    @db.transaction do |db|
      db[:key1] = "hello"
      db[:key2] = "world"
      db.roots.should == [:key1, :key2]
    end
    @db.transaction do |db|
      db.roots.should == [:key1, :key2]
      db.delete(:key1)
      db[:key3] = "!"
      db.roots.should == [:key2, :key3]
    end
    @db.transaction do |db|
      db.roots.should == [:key2, :key3]
    end
  end

  it 'should suppert root?' do
    @db.transaction do |db|
      db.root?(:key).should == false
      db[:key] = "hello"
      db.root?(:key).should == true
    end
    @db.transaction do |db|
      db.root?(:key).should == true
      db.delete(:key)
      db.root?(:key).should == false
    end
    @db.transaction do |db|
      db.root?(:key).should == false
    end
  end

  it 'should support readonly database' do
    p = lambda {
      @db.transaction(true) do |db|
        db[:key] = "hello"
      end
    }
    p.should raise_error PStore::Error
    p = lambda {
      @db.transaction(true) do |db|
        db.delete(:key)
      end
    }
    p.should raise_error PStore::Error
  end

  describe 'transaction' do
    it 'should support commit' do
      @db.transaction do |db|
        db[:key] = "hello"
        db.commit
        # never reached
        raise StandardError
      end
      @db.transaction do |db|
        db[:key].should == "hello"
      end
    end

    it 'should support abort' do
      @db.transaction do |db|
        db[:key] = "hello"
        db.abort
        # never reached
        raise StandardError
      end
      @db.transaction do |db|
        db[:key].should == nil
      end
    end

    it 'should abort transaction if raised any Exception' do
      p = lambda {
        @db.transaction do |db|
          db[:key] = "hello"
          # abort transaction
          raise StandardError
        end
      }
      p.should raise_error StandardError
      @db.transaction do |db|
        db[:key].should == nil
      end
    end

    it 'should not support nested transaction' do
      p = lambda {
        @db.transaction do |db1|
          @db.transaction do |db2|
          end
        end
      }
      p.should raise_error PStore::Error
    end

    it 'should raise PStore::Error outside transaction' do
      p = lambda { @db[:key1] = "hello" }
      p.should raise_error PStore::Error
      p = lambda { @db[:key1] }
      p.should raise_error PStore::Error
      p = lambda { @db.delete(:key1) }
      p.should raise_error PStore::Error
      p = lambda { @db.roots }
      p.should raise_error PStore::Error
    end
  end
end

