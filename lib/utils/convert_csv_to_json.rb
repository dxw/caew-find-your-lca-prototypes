require 'csv'
require 'json'
require 'pry'

options = { headers: true, liberal_parsing: true }

file_location = ARGV[0]
json_objects = []
CSV.foreach(file_location, options) do |row|
  json_objects << JSON.pretty_generate(row.to_hash)
end

file_name = file_location.split("/").last.split(".").first
new_file_location = "tmp/#{file_name}.json"

File.open(new_file_location, 'w') do |f|
  json_objects.map do |object|
    f << object + ","
  end
end

puts "New file added to #{new_file_location}, the content will need to be wrapped in an array."
# ruby lib/utils/convert_csv_to_json.rb tmp/geolocation_data.csv
