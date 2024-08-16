import requests
import smtplib
from email.mime.text import MIMEText

def check_website(url):
    try:
        response = requests.get(url, timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def send_alert(email, message):
    msg = MIMEText(message)
    msg['Subject'] = 'Website Down'
    msg['From'] = 'your_email@example.com'
    msg['To'] = email

    with smtplib.SMTP('smtp.example.com', 587) as server:
        server.starttls()
        server.login('your_email@example.com', 'your_password')
        server.sendmail('your_email@example.com', email, msg.as_string())

websites = ['https://example1.com', 'https://example2.com']
email = 'client_email@example.com'

for site in websites:
    if not check_website(site):
        send_alert(email, f'Website {site} is down!')


'''
Python, который будет периодически проверять доступность сайтов 
с помощью curl или requests и отправлять уведомления по email 
или в мессенджеры (например, через API Telegram или Slack).
'''