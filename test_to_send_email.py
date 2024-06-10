import smtplib
from email.mime.text import MIMEText

smtp_host = 'smtp.gmail.com'
smtp_port = 587
smtp_user = 'your_email_address'
smtp_password = 'your_app_password'

to_email = 'your_email_address'
subject = 'Test Email'
body = 'This is a test email from Airflow.'

msg = MIMEText(body)
msg['Subject'] = subject
msg['From'] = smtp_user
msg['To'] = to_email

try:
    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.sendmail(smtp_user, to_email, msg.as_string())
    server.quit()
    print("Test email sent successfully!")
except Exception as e:
    print(f"Failed to send test email: {e}")
