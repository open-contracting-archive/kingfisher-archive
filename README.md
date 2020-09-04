# kingfisher-archive

This moves data and log files written by [Kingfisher Collect](https://kingfisher-collect.readthedocs.io/en/latest/) to an archive server, after they have been loaded into [Kingfisher Process](https://kingfisher-process.readthedocs.io/en/latest/). 

The data is archived in case we need to re-load it in future.

## Setup

As presently written, the script needs to be run by a user that has:

* Access to the ``collection`` table managed by Kingfisher Process (e.g. with the ``.pgpass`` file)
* Permission to read and delete the data files written by Kingfisher Collect (e.g. with sudo access)

## Run

    python manage.py archive

To see options

    python manage.py archive --help

## S3 

### Getting Credentials

Go to IAM service.

Click users.

Click Add.

Enter a Username.

For "Access type" pick "Programmatic access" and not "AWS Management Console access".

For Set Permissions, don't select anything. Just continue.

No Tags. Just continue.

Review and create.

Copy access key and Secret access key somewhere safe.

Close creation Wizard.

You should see user in the list. 

Click new user name.

Click "Add inline policy"

Click "JSON" tab.

Enter:

    {
       "Version":"2012-10-17",
       "Statement":[
          {
             "Effect":"Allow",
             "Action":[
                "s3:ListBucket"
             ],
             "Resource":"arn:aws:s3:::AWSDOC-EXAMPLE-BUCKET"
          },
          {
             "Effect":"Allow",
             "Action":[
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
             ],
             "Resource":"arn:aws:s3:::AWSDOC-EXAMPLE-BUCKET/*"
          }
       ]
    }

Click Review Policy

Give the policy a name.

Click Create Policy

### Specifying Config and Credentials

For more see
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

What follows describes one of several methods.

Create `~/.aws/config`:

    [default]
    region = us-east-1
    aws_access_key_id = xxxxxxxxxxxxx
    aws_secret_access_key = yyyyyyyyyyyyyyyyyyyy

