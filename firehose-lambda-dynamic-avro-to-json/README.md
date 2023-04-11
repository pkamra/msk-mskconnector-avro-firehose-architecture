execute all commands in the project folder
lambda_firehose_avro_json piykamra$ pip3 install codecs --target=$(pwd)
lambda_firehose_avro_json piykamra$ zip -r script.zip .

Next I went to the lambda console and selected upload zip and selected script.zip
If the size of teh zip is upto 50MB with all dependencies and you dont want to go in for Lambda Layers then thsi method can be used.

>>git remote add origin 
