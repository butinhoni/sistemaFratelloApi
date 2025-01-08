from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import smtplib


def sendmail(sender, login, senha, mensagem):
#assinatura
    img = 'assinatura_erros.png'

    #Corpo do Email
    corpo = f"""
    <html> 
    <head></head>
        <body>
        Olá equipe, espero que esse email os encontre bem. Tivemos a seguinte ocorrência durante a execução da ultima atualização. 
        <br>
        <br>
        {mensagem}
        <br>
        <br>
        Att,
        <br>
        <br>
        <img src="cid:assinatura">
        </body>
    </html>
    """
    msg = MIMEMultipart()
    msg.attach(MIMEText(corpo,'html'))

    with open(img, 'rb') as f:
        image = MIMEImage(f.read())
        image.add_header('Content-ID','<assinatura>')
        msg.attach(image)

    #endereços
    email = 'guilherme.of.fernandes@gmail.com'
    
    #ENVIAR EMAIL
    msg['Subject'] = 'Busca Completa na API'
    msg['From'] = sender
    msg['To'] = email
    with smtplib.SMTP('smtp-mail.outlook.com', 587) as smtp_server:
        smtp_server.starttls()
        smtp_server.login(login,senha)
        smtp_server.sendmail(sender,email.split(","),msg.as_string())
    print("email enviado")

