#!/usr/bin/env python
# coding:utf-8
import os
import mimetypes
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import parseaddr, formataddr
from lib import Utility


# 格式化邮件地址
def format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))


def send_mail(html='', attachments=None):
    smtp_server_host = 'smtp.qq.com'
    smtp_server_ssl_port = 465
    my_email = '1475811550@qq.com'  # 发件人邮箱账号
    my_password = 'code'  # 发件人邮箱密码或授权码
    to_email = ['1475811550@qq.com', '1145813667@qq.com', 'huitt@youzu.com']

    # 构造一个MIMEMultipart对象代表邮件本身
    msg = MIMEMultipart()
    # Header对中文进行转码
    msg['From'] = format_addr('App-Store-Review-Guidelines 差异监控 <%s>' % my_email).encode()
    msg['To'] = ','.join(to_email)
    msg['Subject'] = Header('App-Store-Review-Guidelines 差异监控', 'utf-8').encode()
    # 添加邮件正文
    msg.attach(MIMEText(html, 'html', 'utf-8'))

    # 添加附件就是加上一个MIMEBase
    if isinstance(attachments, list):
        for attachment in attachments:
            if os.path.isfile(attachment):
                # file_extension = os.path.splitext(attachment)[1]
                mime_type = mimetypes.guess_type(attachment)[0]
                major_type = mime_type.split('/')[0]
                sub_type = mime_type.split('/')[1]

                with open(attachment, 'rb') as f:
                    # 设置附件的MIME和文件名，这里是png类型:
                    mime = MIMEBase(major_type, sub_type, filename=attachment)
                    # 加上必要的头信息:
                    mime.add_header('Content-Disposition', 'attachment', filename=attachment)
                    mime.add_header('Content-ID', '<0>')
                    mime.add_header('X-Attachment-Id', '0')
                    # 把附件的内容读进来:
                    mime.set_payload(f.read())
                    # 用Base64编码:
                    encoders.encode_base64(mime)
                    # 添加到MIMEMultipart:
                    msg.attach(mime)
                    Utility.log('成功添加附件：', attachment)
    try:
        server = smtplib.SMTP_SSL(smtp_server_host, smtp_server_ssl_port)
        server.set_debuglevel(1)  # 开启调试信息
        server.login(my_email, my_password)  # 括号中对应的是发件人邮箱账号、授权码
        server.sendmail(my_email, to_email, msg.as_string())
        # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except smtplib.SMTPException as e:
        return "Error: %s" % e
    return True


def main():
    body = """ 
    <h1>测试邮件</h1> 
    <h2 style="color:yellow">This is a test</h1>
    """
    flag = send_mail(body)

    if flag is True:
        print("邮件发送成功")
    else:
        print(flag)


if __name__ == '__main__':
    main()
