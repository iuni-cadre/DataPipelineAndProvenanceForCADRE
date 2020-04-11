https://www.sicara.ai/blog/2018-05-25-build-serverless-rest-api-15-minutes-aws
https://github.com/quentinf00/API-S3-Lambda
https://serverless.com/framework/docs/providers/aws/guide/installation/

sudo aptitude install nodejs
sudo aptitude install npm

npm install -g serverless
serverless config credentials --provider aws --key AKIAJBXFDKLW67XRX5IA --secret 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

sls deploy
sls invoke -f init_athena_schema

-----------fixing nodejs module problem--------
rm -rf node_modules
npm install
