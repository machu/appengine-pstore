#!/usr/bin/env ruby
# Copyright:: Copyright 2009 MATSUOKA Kohei
#  
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   
#     http://www.apache.org/licenses/LICENSE-2.0
#        
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'rubygems'
require 'appengine-apis/datastore'

# from pstore.rb
class PStore
  # The error type thrown by all PStore methods.
  class Error < StandardError
  end
end

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

  # The PStore compatible interface for Google App Engine Datastore.
  # 
  #  db = AppEngine::PStore.new(dbname)
  #  db.transaction do |db|
  #    db[key] = value
  #  end
  # 
  # A data is stored to the Datastore as AppEngine::Entity.
  # The stracture of Datastore has following schema.
  #
  #  * dbname: a root key for the Datastore.
  #  * key: A child of the root key.
  #  * value: AppEngine::Entity that contains the value object.
  #
  # Note: 'key' and 'value' are marshalled before putting Datastore.
  #
  class PStore
    # Create a database identified by dbname.
    def initialize(dbname)
      @kind = self.class.name
      @parent_key = AppEngine::Datastore::Key.from_path(@kind, dbname)
      @transaction = nil
    end

    # Raises PStore::Error unless in a transaction.
    def in_transaction
      if @transaction == nil || @transaction.active? == false
        raise ::PStore::Error, "not in transaction"
      end
    end

    # Raises PStore::Error unless in a writable transaction.
    def in_transaction_wr
      in_transaction
      raise ::PStore::Error, "in read-only transaction" if @rdonly
    end
    private :in_transaction, :in_transaction_wr

    # Begins a new transaction for the AppEngine::Datastore.
    def transaction(readonly = false)
      raise ::PStore::Error, "nested transaction" if @transaction
      @rdonly = readonly
      @transaction = AppEngine::Datastore.begin_transaction
      # uncommited entities
      @uncommited = {
        :added => {},
        :deleted => {}
      }
      begin
        catch(:pstore_abort_transaction) do
          yield self
        end
      rescue Exception
        @transaction.rollback if @transaction.active?
        raise
      ensure
        @transaction.commit if @transaction.active?
        @transaction = nil
        @uncommited = nil
      end
    end

    # Commit the transaction.
    def commit
      in_transaction
      @transaction.commit
      throw :pstore_abort_transaction
    end

    # Abort the transaction.
    def abort
      in_transaction
      @transaction.rollback
      throw :pstore_abort_transaction
    end

    # Retrieves a stored value from the Datastore by the name.
    def [](name)
      in_transaction
      # return uncommited data if exist
      return @uncommited[:added][name] if @uncommited[:added].key?(name)
      return nil if @uncommited[:deleted].key?(name)

      key = AppEngine::Datastore::Key.from_path(@parent_key, @kind, dump(name))
      begin
        entity = AppEngine::Datastore.get(@transaction, key)
        load(entity[:value])
      rescue AppEngine::Datastore::EntityNotFound
        nil
      end
    end

    # Stores a value to the Datastore by the name.
    def []=(name, value)
      in_transaction_wr
      entity = AppEngine::Datastore::Entity.new(@kind, dump(name), @parent_key)
      entity[:value] = dump(value)
      AppEngine::Datastore.put(@transaction, entity)
      @uncommited[:added][name] = value
      @uncommited[:deleted].delete(name)
      value
    end

    # Delete a value from the Datastore by the name.
    def delete(name)
      in_transaction_wr
      key = AppEngine::Datastore::Key.from_path(@parent_key, @kind, dump(name))
      value = self[name]
      # Datastore.delete requires array keys
      AppEngine::Datastore.delete(@transaction, [key])
      @uncommited[:added].delete(name)
      @uncommited[:deleted][name] = value
      value
    end

    def load(content)
      Marshal::load(content)
    end

    def dump(content)
      Marshal::dump(content)
    end

    # Returns all keys of this database.
    def roots
      in_transaction
      query = AppEngine::Datastore::Query.new(@kind, @parent_key)
      db_keys = query.keysonly.fetch.map {|entity|
        load(entity.key.name)
      }
      (db_keys + @uncommited[:added].keys - @uncommited[:deleted].keys).uniq
    end

    # Whether the database has key.
    def root?(key)
      in_transaction
      self.roots.include?(key)
    end

    # Returns the database's name
    def path
      @parent_key.name
    end
  end
end

