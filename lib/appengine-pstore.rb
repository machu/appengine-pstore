require 'rubygems'
require 'appengine-apis/datastore'

module AppEngine
  module Datastore
    class Query
      def keysonly
        @query.set_keys_only
        self
      end

      def keysonly?
        @query.is_keys_only
      end
    end
  end

  class PStore
    def initialize(dbname)
      @kind = self.class.name
      @parent_key = AppEngine::Datastore::Key.from_path(@kind, dbname)
      @transaction = nil
    end

    def in_transaction
      unless AppEngine::Datastore.current_transaction(nil)
        raise PStore::Error, "not in transaction"
      end
    end

    def in_transaction_wr
      in_transaction
      raise PStore::Error, "in read-only transaction" if @rdonly
    end
    private :in_transaction, :in_transaction_wr

    def transaction(readonly = false)
      @rdonly = readonly
      @transaction = AppEngine::Datastore.begin_transaction
      @cache = {}
      yield self
      @transaction.commit
      @transaction = nil
    end

    def [](name)
      in_transaction
      raise PStore::Error, "in read-only transaction" if @rdonly
      if @cache.key?(name)
        @cache[name]  # return latest put data
      else
        key = AppEngine::Datastore::Key.from_path(@parent_key, @kind, name)
        entity = AppEngine::Datastore.get(@transaction, key)
        load(entity[:value])
      end
    end

    def []=(name, value)
      in_transaction_wr
      entity = AppEngine::Datastore::Entity.new(@kind, name, @parent_key)
      entity[:value] = dump(value)
      AppEngine::Datastore.put(@transaction, entity)
      @cache[name] = value
      value
    end

    def delete(name)
      key = AppEngine::Datastore::Key.from_path(@parent_key, @kind, name)
      AppEngine::Datastore.delete(@transaction, key)
      @cache.delete(name)
    end

    def load(content)
      Marshal::load(content)
    end

    def dump(content)
      Marshal::dump(content)
    end

    def roots
      in_transaction
      query = AppEngine::Datastore::Query.new(@kind, @parent_key)
      query.keysonly.fetch.map {|value|
        value.key.name
      }.concat(@cache.keys).uniq
    end

    def root?(key)
      in_transaction
    end

    def path
      @parent_key
    end
  end
end

