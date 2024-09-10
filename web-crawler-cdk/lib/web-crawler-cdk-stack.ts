import * as cdk from "aws-cdk-lib";
import { Stack, StackProps } from "aws-cdk-lib";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { Function, Runtime, Code } from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import { Topic } from "aws-cdk-lib/aws-sns";
import { SnsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { PythonFunction } from "@aws-cdk/aws-lambda-python-alpha";
import { Construct } from "constructs";

export class WebCrawlerCdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // S3 bucket to store ASINs and output data
    const asinBucket = new Bucket(this, "AsinBucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // SNS Topic for job splitter
    const snsTopic = new Topic(this, "SnsTopic", {
      displayName: "Job Splitter SNS Topic",
    });

    // Job Splitter Lambda
    const jobSplitterLambda = new PythonFunction(this, "JobSplitterLambda", {
      runtime: Runtime.PYTHON_3_9,
      entry: "lambda",
      index: "job_splitter.py",
      handler: "handler",
      environment: {
        BUCKET_NAME: asinBucket.bucketName,
        SNS_TOPIC_ARN: snsTopic.topicArn,
      },
    });

    // Crawler Lambda
    const crawlerLambda = new PythonFunction(this, "CrawlerLambda", {
      runtime: Runtime.PYTHON_3_9,
      entry: "lambda",
      index: "crawler.py",
      handler: "handler",
      environment: {
        BUCKET_NAME: asinBucket.bucketName,
      },
    });

    // Grant permissions
    asinBucket.grantReadWrite(jobSplitterLambda);
    asinBucket.grantReadWrite(crawlerLambda);
    snsTopic.grantPublish(jobSplitterLambda);

    // Subscribe Crawler Lambda to SNS topic
    crawlerLambda.addEventSource(new SnsEventSource(snsTopic));

    // Set up IAM Policy for Lambdas
    const lambdaPolicy = new iam.PolicyStatement({
      actions: ["s3:GetObject", "s3:PutObject", "sns:Publish"],
      resources: [`${asinBucket.bucketArn}/*`, snsTopic.topicArn],
      effect: iam.Effect.ALLOW,
    });

    jobSplitterLambda.addToRolePolicy(lambdaPolicy);
    crawlerLambda.addToRolePolicy(lambdaPolicy);
  }
}
