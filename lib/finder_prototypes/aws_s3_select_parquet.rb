require 'benchmark'

module FinderPrototypes
  class AwsS3SelectParquet
    def initialize
      @client = Aws::S3::Client.new(region: 'eu-west-2')
    end

    def find_postcode_params(postcode='BS4 3JB')
      {
        bucket:"caew-find-lca-test",
        # The Glue spits out 2 files and depending on the postcode you might
        # get a miss or a match and need to check the other file..
        # run-S3bucket_node3-2-part-block-0-r-00002-gzip.parquet
        key: "run-S3bucket_node3-2-part-block-0-r-00001-gzip.parquet",
        expression_type: 'SQL',
        expression: "SELECT * FROM S3Object WHERE POSTCODE='#{postcode}'",
        input_serialization: {
          compression_type: 'NONE',
          parquet: {
          }
        },
        output_serialization: {
          json: {
          }
        }
      }
    end

    def find_office_params(id='0014K00000b5dOTQAY')
      {
        bucket:"caew-find-lca-test",
        key: "run-S3bucket_node3-2-part-block-0-r-00000-snappy.parquet",
        expression_type: 'SQL',
        expression: "SELECT * FROM S3Object WHERE Id='#{id}'",
        input_serialization: {
          compression_type: 'NONE',
          parquet: {
          }
        },
        output_serialization: {
          json: {
          }
        }
      }
    end

    def call
      Benchmark.bm do |x|
        x.report("Query location Parquet:") { @client.select_object_content(find_postcode_params) }
      end
      resp = @client.select_object_content(find_postcode_params)
      result = resp.payload.to_a.detect{|data| data.event_type == :records}.payload.readline
      # puts JSON.parse(result)


      Benchmark.bm do |x|
        x.report("Query office Parquet:") { @client.select_object_content(find_office_params) }
      end
      resp = @client.select_object_content(find_office_params)
      result = resp.payload.to_a.detect{|data| data.event_type == :records}.payload.readline
      # puts JSON.parse(result)
    end
  end
end
