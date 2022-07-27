require 'benchmark'

module FinderPrototypes
  class AwsS3SelectParquet
    def initialize
      @client = Aws::S3::Client.new(region: 'eu-west-2')
    end

    def find_postcode_params(postcode='BS4 3JB')
      {
        bucket: "caew-find-lca-test",
        key: "geolocations/parquet/renamed/data.parquet",
        expression_type: 'SQL',
        expression: "SELECT * FROM S3Object WHERE postcode__c = '#{postcode}'",
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

    def find_local_authority_params(id)
      {
        bucket: "caew-find-lca-test",
        key: "local-authorities/parquet/renamed/data.parquet",
        expression_type: 'SQL',
        expression: "SELECT * FROM S3Object WHERE id = '#{id}'",
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
        bucket: "caew-find-lca-test",
        key: "offices/parquet/renamed/data.parquet",
        expression_type: 'SQL',
        expression: "SELECT * FROM S3Object WHERE Local_Authority__c='#{id}'",
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
      json_result = JSON.parse(result)
      local_authority_id = json_result["local_authority__c"]

      Benchmark.bm do |x|
        x.report("Query local authority Parquet:") { @client.select_object_content(find_local_authority_params(local_authority_id)) }
      end
      resp = @client.select_object_content(find_local_authority_params(local_authority_id))
      result = resp.payload.to_a.detect{|data| data.event_type == :records}.payload.readline
      json_result = JSON.parse(result)
      # TODO: Find out what data is needed from local authorities

      Benchmark.bm do |x|
        x.report("Query office Parquet:") { @client.select_object_content(find_office_params(local_authority_id)) }
      end
      resp = @client.select_object_content(find_office_params(local_authority_id))
      result = resp.payload.to_a.detect{|data| data.event_type == :records}.payload.readline
      puts JSON.parse(result)
    end
  end
end
