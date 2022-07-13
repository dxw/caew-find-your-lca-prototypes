require 'benchmark'

module FinderPrototypes
  class AwsS3SelectJson
    def initialize
      @client = Aws::S3::Client.new(region: 'eu-west-2')
    end

    def find_postcode_params(postcode='BS4 3JB')
      {
        bucket:"caew-find-lca-test",
        key: "geolocation_data.json",
        expression_type: 'SQL',
        expression: "SELECT Postcode FROM S3Object[*].Locations[*].Postcode WHERE Postcode='#{postcode}'",
        input_serialization: {
          compression_type: 'NONE',
          json: {
            type: 'DOCUMENT',
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
        key: "offices_and_outreaches.json",
        expression_type: 'SQL',
        expression: "SELECT s.Id, s.Local_Authority__c, s.Membership_Number__c Name, s.ParentId, s.BillingStreet, s.BillingState, s.BillingCity, s.BillingPostalCode, s.BillingLatitude, s.BillingLongitude, s.Website, s.Phone, s.About_our_advice_service__c, s.Email__c, s.Access_details__c, s.Local_Office_Opening_Hours_Information__c, s.Telephone_Advice_Hours_Information__c, s.Closed__c, s.LastModifiedDate, s.RecordTypeId FROM S3Object[*].Offices[*] s WHERE s.Id='#{id}'",
        input_serialization: {
          compression_type: 'NONE',
          json: {
            type: 'DOCUMENT',
          }
        },
        output_serialization: {
          json: {
          }
        }
      }
    end

    def call
      # INFO: ~2-3 seconds is slow, too slow. Which confirms what AWS said
      # but puts it much slower than CSV which was also defined as "slow".
      Benchmark.bm do |x|
        x.report("Query geolocation JSON:") { @client.select_object_content(find_postcode_params) }
      end

      resp = @client.select_object_content(find_postcode_params)
      result = resp.payload.to_a.detect{|data| data.event_type == :records}.payload.readline

      # TODO: Right now we pull the postcode because there isn't an LA
      # identifier in the dataset I'm working with. This needs to be properly
      # extracted and fed into the following query. (unless these datasets are
      # merged upstream)
      # puts JSON.parse(result)

      Benchmark.bm do |x|
        x.report("Query office JSON:") { @client.select_object_content(find_office_params) }
      end

      resp = @client.select_object_content(find_office_params)
      result = resp.payload.to_a.detect{|data| data.event_type == :records}.payload.readline

      # Jbuilder could be used to render this
      # puts JSON.parse(result)
    end
  end
end
