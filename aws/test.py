import boto3

try:
    # Create an STS client
    sts_client = boto3.client('sts')

    # Call get_caller_identity to retrieve information about the current identity
    response = sts_client.get_caller_identity()

    # Print relevant information from the response
    print(f"AWS Account ID: {response['Account']}")
    print(f"User ARN: {response['Arn']}")
    print(f"User ID: {response['UserId']}")

except Exception as e:
    print(f"Error checking credentials: {e}")
    print("Please ensure your AWS credentials are configured correctly.")