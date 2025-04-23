# Deployment Document

## Step 1 - Set up Roles and Policy

### Roles
 - 1. lambda-role - 
    ```{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "customCode",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:CreateSecret",
                "secretsmanager:PutSecretValue",
                "secretsmanager:TagResource",
                "secretsmanager:GetRandomPassword",
                "secretsmanager:GetResourcePolicy",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecretVersionIds",
                "secretsmanager:ListSecrets",
                "secretsmanager:BatchGetSecretValue"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "rds:*",
                "application-autoscaling:DeleteScalingPolicy",
                "application-autoscaling:DeregisterScalableTarget",
                "application-autoscaling:DescribeScalableTargets",
                "application-autoscaling:DescribeScalingActivities",
                "application-autoscaling:DescribeScalingPolicies",
                "application-autoscaling:PutScalingPolicy",
                "application-autoscaling:RegisterScalableTarget",
                "cloudwatch:DescribeAlarms",
                "cloudwatch:GetMetricStatistics",
                "cloudwatch:PutMetricAlarm",
                "cloudwatch:DeleteAlarms",
                "cloudwatch:ListMetrics",
                "cloudwatch:GetMetricData",
                "sns:ListSubscriptions",
                "sns:ListTopics",
                "sns:Publish",
                "logs:DescribeLogStreams",
                "logs:GetLogEvents",
                "outposts:GetOutpostInstanceTypes",
                "devops-guru:GetResourceCollection"
            ],
            "Resource": "*"
        },
        {
            "Action": [
                "healthlake:*",
                "s3:ListAllMyBuckets",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "iam:ListRoles"
            ],
            "Resource": "*",
            "Effect": "Allow"
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": "healthlake.amazonaws.com"
                }
            }
        },
        {
            "Sid": "Statement1",
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": "arn:aws:lambda:us-east-1:590183870986:function:getAuthorizationToken"
        },
        {
            "Effect": "Allow",
            "Action": "events:*",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "states:InvokeHTTPEndpoint",
            "Resource": "arn:aws:states:us-east-1:590183870986:stateMachine:*"
        },
        {
            "Effect": "Allow",
            "Action": "events:RetrieveConnectionCredentials",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kms:GenerateDataKey"
            ],
            "Resource": "arn:aws:kms:us-east-1:590183870986:key/b6b338b7-cb46-40f2-a0b7-b71cc9282bf5"
        }
    ]
}

## Step 2 - Create RDS MySQL database - 

Follow step here to create MySQL db on RDS, name the db - Wintergreen-onboarding 
https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_GettingStarted.CreatingConnecting.MySQL.html
 
 Note - Remember the Host url of the DB.
## Step 3 - Create Lambda function 
Lambda functions are in the Lambda function Folder, create all lambda functions and then connect RDS databse to the functions interacting with the database. Add role "lambda-role" to all the functions. Add environment variables on lambda functions interacting with  db.  

### environment Variables - 
 - DB_NAME - wintergreen-onboarding
 - HOST - "paste the host url of the DB created in last step" 
 - USERNAME - admin
 - PASSWORD - "password you created for DB"

 - Run create_table_lambda to set up tables in RDS.

## Step 3 - Create Step Function (State Machine)

make step function that calls initiate_bulk_fhir_export lambda function, this functio nwill return polling location url, pass this to get_bulk_fhir_export_status lambda function , check the status returned , if 202 re try after waiting ofr 300 seconds and if 200, call get_patient_data lambda function. 

## Step 4 - Deploy Front End

Fork the Fronty end git repo to your own github account, go to ASW Amplify and deploy from github repo, Add build command ```npm install``` 

## Step 5 - Connect AWS Cognito to Front end

Go to AWS cognito, create a UserPool. configte callback and signout url basedon the deployed front end. Import UserPool into amplify. re-deploy ampilfy front-end.

## step 6 - create API 

In API gateway make API endpoints being called from the front-end, connect the endpoints to Lambda functions and Step function.


# congratulation, Setup for the Application is now complete. 