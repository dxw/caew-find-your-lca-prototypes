require "dotenv"
Dotenv.load

require "pry"
require "aws-sdk-s3"

require_relative "finder_prototypes/aws_s3_select_csv"
require_relative "finder_prototypes/aws_s3_select_json"
require_relative "finder_prototypes/aws_s3_select_parquet"
require_relative "finder_prototypes/arrow_to_parquet"
module FinderPrototypes
end
