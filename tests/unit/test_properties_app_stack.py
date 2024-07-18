import aws_cdk as core
import aws_cdk.assertions as assertions

from properties_app.properties_app_stack import PropertiesAppStack

# example tests. To run these tests, uncomment this file along with the example
# resource in properties_app/properties_app_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = PropertiesAppStack(app, "properties-app")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
