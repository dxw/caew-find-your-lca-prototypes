
module FinderPrototypes
  class ArrowToParquet
    def self.call
      require 'fileutils'
      FileUtils.mkdir_p('tmp/local-authorities/parquet')
      FileUtils.mkdir_p('tmp/offices/parquet')
      FileUtils.mkdir_p('tmp/geolocations/parquet')

      # I can't get red-arrow to accept a bucket uri. Perhaps I can confirm if I
      # give it a local one that I've downloaded first?
      require 'arrow-dataset'
      require 'cgi/util'

      s3 = Aws::S3::Client.new(region: 'eu-west-2')
      # This is only for local authorities, this approach would need this
      # written out again for offices and geolocations.
      resp = s3.list_objects_v2(
        bucket: 'caew-find-lca-test',
        delimiter: "",
        start_after: "local-authorities/parquet/",
        prefix: "local-authorities/parquet/")

      tables = []
      resp.contents.each do |object_contents|
        # Write to a file as holding all the geolocations in memory is going
        # to be more resource intensive.
        File.open("tmp/#{object_contents.key}", 'wb') do |file|
          reap = s3.get_object({ bucket:'caew-find-lca-test', key: object_contents.key }, target: file)
        end

        tables << Arrow::Table.load(URI("tmp/#{object_contents.key}"), format: :parquet)
      end

      # Might be easier to stick with loading a single CSV file.
      # Parquet introduces a schema that should improve data integrity, but is
      # it worth the cost? Here we are downloading and recreating each parquet table
      # but we can't query with SQL.
      tables.each_with_index do |table, index|
        break if index+1 == tables.count
        tables[0] = tables[0].merge(tables[index+1])
      end
      merged_table = tables[0]

      # This doesn't seem to work well at all!
      # With S3 Select we can ask for the results to get converted into JSON
      # Arrow::Table isn't great to work with. Docs are limited and the source
      # takes us into a lot of meta programming, e.g show-source table.filter_raw
      # => gobject-introspection-3.5.1/lib/gobject-introspection/loader
      result = merged_table.slice { |slicer| slicer['local_authority__c'] == "0014K000009EGkuQAG" }
    end
  end
end
