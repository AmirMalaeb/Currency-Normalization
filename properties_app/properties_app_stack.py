from aws_cdk import (
    Duration,
    Stack,
    aws_s3 as s3,
    aws_s3_notifications as s3_notif,
    RemovalPolicy,
    aws_lambda,
    aws_dynamodb as dynamodb
)

from constructs import Construct


class PropertiesAppStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3
        buckets = {'raw_properties': None, 'processed_properties': None}
        for id in buckets:
            buckets[id] = self.create_bucket(id)

        # DYNAMODB
        table = dynamodb.Table(self, 'PropertiesTable',
                               partition_key=dynamodb.Attribute(
                                   name='zpid',
                                   type=dynamodb.AttributeType.STRING
                               ),
                               removal_policy=RemovalPolicy.DESTROY,
                               billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
                               )

        # LAMBDA
        lambda_cn = aws_lambda.Function(self, 'CurrencyNormalizer',
                                        runtime=aws_lambda.Runtime.PYTHON_3_10,
                                        timeout=Duration.seconds(10),
                                        handler='lambda_function.lambda_handler',
                                        code=aws_lambda.Code.from_asset("src/CurrencyNormalizer/")
                                        )

        # PERMISSIONS & EVENTS
        buckets['raw_properties'].add_event_notification(s3.EventType.OBJECT_CREATED,
                                                         s3_notif.LambdaDestination(lambda_cn))
        buckets['raw_properties'].grant_read_write(lambda_cn)
        buckets['processed_properties'].grant_read_write(lambda_cn)

        table.grant_write_data(lambda_cn)

        # ENVIRONMENTAL VARS
        lambda_cn.add_environment('RAW_PROPERTIES_BUCKET', buckets['raw_properties'].bucket_name)
        lambda_cn.add_environment('PROCESSED_PROPERTIES_BUCKET', buckets['processed_properties'].bucket_name)
        lambda_cn.add_environment('PROPERTIES_TABLE_NAME', table.table_name)
        lambda_cn.add_environment('EXCHANGE_API_KEY', '7390b63dddbdd13a5b9b01800ff98ccd')

    def create_bucket(self, id):
        bucket = s3.Bucket(self, id,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True
                           )

        return bucket