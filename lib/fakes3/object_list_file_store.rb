module FakeS3
  class S3MatchSet
    attr_accessor :matches,:is_truncated
    def initialize
      @matches = []
      @is_truncated = false
    end
  end

  class ObjectListFileStore
    
    def initialize(bucket_name, bucket_path, file_store)
      @bucket_name = bucket_name
      @bucket_path = bucket_path
      @file_store = file_store
    end

    def count
      counter = 0
      @file_store.object_list(@bucket_name, @bucket_path) do |s3_object|
        counter+=1
      end
      counter
    end

    def find(object_name)
      @file_store.object_list(@bucket_name, @bucket_path) do |s3_object|
        if s3_object.name == object_name
          return s3_object
        end
      end
      nil
    end

    def list(options)
      marker = options[:marker]
      prefix = options[:prefix]
      max_keys = options[:max_keys] || 1000
      delimiter = options[:delimiter]

      ms = S3MatchSet.new

      marker_found = true

      count = 0
      @file_store.object_list(@bucket_name, @bucket_path) do |s3_object|
        if marker_found && (!prefix or s3_object.name.index(prefix) == 0)
          count += 1
          if count <= max_keys
            ms.matches << s3_object
          else
            is_truncated = true
            break
          end
        end

        if marker and marker == s3_object.name
          marker_found = true
        end
      end

      return ms
    end
  end
end