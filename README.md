# Scala_Application
This applications reads the S3 bucket and Write the file back to S3 bucket as TSV file.

## Get Started
To get started, open `~/.aws/credentials` and initialise the AWS credentials and Create a `application.conf` file under `src/main/resources/`.

`app {
   inputPath : "<enter the S3 inputPath>"
   outputPath : "<enter the S3 outputPath>"
  }`
  
  The application.conf contains configurations in the form of key-value pair.
