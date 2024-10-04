import aws_cdk as core
import aws_cdk.assertions as assertions

from aws_chat.aws_chat_stack import AwsChatStack

# example tests. To run these tests, uncomment this file along with the example
# resource in aws_chat/aws_chat_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AwsChatStack(app, "aws-chat")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
