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

  class PStore
    def initialize(dbname)
      @kind = self.class.name
      @parent_key = AppEngine::Datastore::Key.from_path(@kind, dbname)
      @transaction = nil
    end

    def in_transaction
      #transaction = AppEngine::Datastore.current_transaction(nil)
      #pp transaction.active?
      #pp @transaction.active?
      if @transaction == nil || @transaction.active? == false
      #unless transaction && transaction.active
      #unless AppEngine::Datastore.current_transaction(nil)
        raise ::PStore::Error, "not in transaction"
      end
    end

    def self.active_transactions
      AppEngine::Datastore.active_transactions
    end

    def in_transaction_wr
      in_transaction
      raise ::PStore::Error, "in read-only transaction" if @rdonly
    end
    private :in_transaction, :in_transaction_wr

    def transaction(readonly = false)
      @rdonly = readonly
      @transaction = AppEngine::Datastore.begin_transaction
      @abort = false
      # cache uncommited entity
      @uncommited = {
        :added => {},
        :deleted => {}
      }
      begin
        catch(:pstore_abort_transaction) do
          yield self
        end
      rescue Exception
        @abort = true
        raise
      ensure
        if @abort
          @transaction.rollback
        else
          @transaction.commit
        end
        @transaction = nil
      end
    end

    def commit
      in_transaction
      throw :pstore_abort_transaction
    end

    def abort
      in_transaction
      @abort = true
      throw :pstore_abort_transaction
    end

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

    def []=(name, value)
      in_transaction_wr
      entity = AppEngine::Datastore::Entity.new(@kind, dump(name), @parent_key)
      entity[:value] = dump(value)
      AppEngine::Datastore.put(@transaction, entity)
      @uncommited[:added][name] = value
      @uncommited[:deleted].delete(name)
      value
    end

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

    def roots
      in_transaction
      query = AppEngine::Datastore::Query.new(@kind, @parent_key)
      db_keys = query.keysonly.fetch.map {|entity|
        load(entity.key.name)
      }
      (db_keys + @uncommited[:added].keys - @uncommited[:deleted].keys).uniq
    end

    def root?(key)
      in_transaction
      self.roots.include?(key)
    end

    def path
      @parent_key
    end
  end
end

