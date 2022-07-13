require 'benchmark'
require 'CSV'

module FinderPrototypes
  class AwsS3SelectCsv
    def initialize
      @client = Aws::S3::Client.new(region: 'eu-west-2')
    end

    def find_postcode_params(postcode='BS4 3JB')
      {
        bucket:"caew-find-lca-test",
        key: "Geolocation data 3.csv",
        expression_type: 'SQL',
        expression: "SELECT * FROM S3Object WHERE Postcode='#{postcode}'",
        input_serialization: {
          csv: { file_header_info: 'USE'}
        },
        output_serialization: {
          csv: {}
        }
      }
    end

    def find_office_params(id='0014K000009EMHoQAO')
      {
        bucket:"caew-find-lca-test",
        key: "Offices and Outreaches.csv",
        expression_type: 'SQL',
        expression: "SELECT * FROM S3Object WHERE Id='#{id}'",
        input_serialization: {
          csv: {
            file_header_info: 'USE',
            allow_quoted_record_delimiter: true
          }
        },
        output_serialization: {
          csv: {}
        }
      }
    end

    def call
      Benchmark.bm do |x|
        x.report("Query geolocation CSV:") { @client.select_object_content(find_postcode_params) }
      end

      resp = @client.select_object_content(find_postcode_params)
      row_data = CSV.parse(resp.payload&.first&.first&.readline)
      authority_account_name = row_data[0][5]
      # TODO: Can't figure out how to use any of this data to associate to office data

      Benchmark.bm do |x|
        x.report("Query office data CSV:") { @client.select_object_content(find_office_params) }
      end
      resp2 = @client.select_object_content(find_office_params)

      # TODO: A lot of data is invalid as it includes unescaped commas
      # in the notes field. Use a specific ID that gets around this.
      #
      #  https://datatracker.ietf.org/doc/html/rfc4180
      #  "Fields containing line breaks (CRLF), double quotes, and commas
      #  should be enclosed in double-quotes."
      row_data = CSV.parse(resp2.payload.first.payload.readline)
    end
  end
end
