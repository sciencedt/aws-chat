from aws_cdk import (
    Stack,
    aws_apigatewayv2 as apigatewayv2,
    aws_apigatewayv2_integrations as integrations,  # Use the stable module
    aws_lambda as _lambda,
    aws_iam as iam,  # Import IAM
    aws_dynamodb as dynamodb,  # Import DynamoDB
)
from constructs import Construct

class AwsChatStack(Stack):  # Use AwsChatStack as the class name

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create a Lambda function that will handle WebSocket events
        lambda_function = _lambda.Function(
            self, 'WebSocketHandler',
            function_name="chat_handler",
            runtime=_lambda.Runtime.PYTHON_3_9,  # You can change to the runtime you prefer
            handler='websocket_handler.lambda_handler',
            code=_lambda.Code.from_asset('lambda_code')  # Path to the lambda directory
        )

        # Create a separate IAM role for DynamoDB access
        dynamo_role = iam.Role(
            self, 'DynamoDBAccessRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),  # Role is assumed by Lambda
        )

        # Attach DynamoDB full access policy to the role
        dynamo_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('AmazonDynamoDBFullAccess')
        )

        # Attach the role to the Lambda function
        lambda_function.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:*"],  # Allow all DynamoDB actions
            resources=["*"]  # You can specify the ARN of the DynamoDB table if needed
        ))

        # Add permission for managing WebSocket connections
        lambda_function.add_to_role_policy(iam.PolicyStatement(
            actions=["execute-api:ManageConnections"],
            resources=["*"]  # You can restrict this to a specific API if needed
        ))


        # Create a DynamoDB table with PK and SK
        table = dynamodb.Table(
            self, 'inbox',
            table_name="inbox",
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,  # Set to on-demand
            partition_key=dynamodb.Attribute(name='PK', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name='SK', type=dynamodb.AttributeType.STRING),
        )

        # Grant the Lambda function read/write access to the DynamoDB table
        table.grant_read_write_data(lambda_function)

        # Create a DynamoDB table with PK and SK
        table2 = dynamodb.Table(
            self, 'connections',
            table_name="connections",
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,  # Set to on-demand
            partition_key=dynamodb.Attribute(name='PK', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name='SK', type=dynamodb.AttributeType.STRING),
        )

        # Grant the Lambda function read/write access to the DynamoDB table
        table2.grant_read_write_data(lambda_function)

                # Create a DynamoDB table with PK and SK
        table3 = dynamodb.Table(
            self, 'messages',
            table_name="messages",
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,  # Set to on-demand
            partition_key=dynamodb.Attribute(name='PK', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name='SK', type=dynamodb.AttributeType.STRING),
        )

        # Grant the Lambda function read/write access to the DynamoDB table
        table3.grant_read_write_data(lambda_function)

        # Create WebSocket API
        websocket_api = apigatewayv2.WebSocketApi(
            self, 'WebSocketApi',
            connect_route_options=apigatewayv2.WebSocketRouteOptions(
                integration=integrations.WebSocketLambdaIntegration('ConnectIntegration', lambda_function)
            ),
            disconnect_route_options=apigatewayv2.WebSocketRouteOptions(
                integration=integrations.WebSocketLambdaIntegration('DisconnectIntegration', lambda_function)
            ),
            default_route_options=apigatewayv2.WebSocketRouteOptions(
                integration=integrations.WebSocketLambdaIntegration('DefaultIntegration', lambda_function)
            ),
        )

        # Create WebSocket stage
        stage = apigatewayv2.WebSocketStage(
            self, 'WebSocketStage',
            web_socket_api=websocket_api,
            stage_name="dev",
            auto_deploy=True  # Automatically deploy changes
        )

        # Output the WebSocket API URL
        self.api_url = stage.callback_url

        # Optionally, you can output the WebSocket API endpoint
        print(f"WebSocket API endpoint: {self.api_url}")
