from redmail import EmailSender

class Emailer():

    def __init__(self):
        self.smtp_ip = "10.10.12.180"
        self.smtp_port = "25"

    def send_it(self,receivers: list, subject, content, email_cc: list = [], attachment_path = None, body_images = None, body_tables = None):

        email = EmailSender(
                    host=self.smtp_ip,
                    port=self.smtp_port,
                    use_starttls=False,
                )

        email.connect()

        email.send(
            subject=subject,
            sender='noreply@zuelligpharma.com',
            receivers= receivers,
            cc=email_cc,
            html=content,
            attachments=attachment_path,
            body_images=body_images,
            body_tables=body_tables,
        )