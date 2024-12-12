from airflow.models import Variable
from airflow.hooks.base import BaseHook

""" s3 = boto3.client(
    's3',
    region_name='us-east-1',  # Ajuste para sua região
    aws_access_key_id='ASIAWAA66KBXXMU7KCLM',
    aws_secret_access_key='PBXsQ8i77tY6gW+O8GsfPjWY6fOtWEqiZYLibkPi',
    aws_session_token='IQoJb3JpZ2luX2VjEAUaCXVzLWVhc3QtMSJHMEUCIFga5Wc35Ffov/cscUUVdqMFdQpXvwRUXjK6pxxXy8WjAiEA5stWe8g1Wx3CsPGebJvXUzfkymeKn/OTRSEoWCRPklUqsAMIvv//////////ARAAGgw0MTIzODE3NjE2NDciDFnbKcThE/lWrRYdwSqEA+mVYTCWK2rXzHjyFAEJO7U6IY477w6v36M1wmhVMTELxRhzqYW/Ch/q1ZGRMOJ3DhtgyHX8B4vNFXcs+8E317fJeAyEYtJMS4Psg8fMe9L/dehOoQ9OEwjEO+9sB1zGjbusF7/Nbv1merLqNDb5Aum2otlbarSXldnFk2XBHo3oBvEY+tG8H0doTLlhNO2NmWX8IgntCF0c++IPVm/62LEoIlGTB3IjGCXu2QTZI3UpW3U0nt2kP1vywdtjiZmxSpO/kCO+IftLgG8E+s8pnSb50R8t+f1lRHWG5yQ8rUkGKuemQ6Kapsqhri3r4znrqlnIqYUslnK+7sDAucsz0Vgx/kq64YssCuj0kaENn1vtZMeM4/6gIpSV65uQE+z3BKaAhuAh7jFvEXUj+5J3OYVHD0DO/lUXlHEKCU6wdFtnpWwGmafe3JcYYjadHy3pSIJeniQtiJKyde0R3/n/dRv3xkRGWgKjkI6xSoAt/PJoclKrFZkxcagZ/rhKMR9E8m/rA20wi7XrugY6pgGx0W1lNy5gquFw4dFx8a9GH/ZnCLRRBbUCgQZOXdPNpQ5R3VaqJuowIcJtg10yUYL/JobmZIZ61Zjqyrp7TmBgYxlKcKs+RfRK4uxvup0d9W5IOnMlp5+mUHvu9Ysu+RHDbd1j2PdHUguB96aPqjBFeGuX5IjOJPQ0O6w71QNkdq+xewpHRIHWV6SrmhK5J7nPeHM77rYES2xpSLnlLDyflu/N/gfv'
)
response = s3.list_buckets() """


aws_conn = BaseHook.get_connection('aws_default')
aws_access_key = aws_conn.login
aws_secret_key = aws_conn.password
aws_session_token = Variable.get("AWS_SESSION_TOKEN", default_var="")

print(aws_access_key, aws_secret_key, aws_session_token)
