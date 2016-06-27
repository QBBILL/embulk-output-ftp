package org.embulk.output.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TestFtpOutputPlugin
{
    public static void main(String[] args) throws IOException {
        System.out.println(String.format(".%03d", 1, 2));
        FTPClient ftp = new FTPClient();
        int reply;
        ftp.connect("192.168.91.160");
        ftp.login("ftpuser", "123456");
        System.out.println(ftp.getReplyCode());
        reply = ftp.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            ftp.disconnect();
            System.err.println("FTP server refused connection.");
            System.exit(1);
        }
        InputStream is = new FileInputStream("C:/Users/钱斌/Desktop/新建文本文档.txt");
        OutputStream os = ftp.storeFileStream("/home/ftpuser/6.txt");
        byte[] buffer = new byte[1024];
        int len;
        while((len = is.read(buffer)) != -1) {
            os.write(buffer, 0, len);
        }

        is.close();

        os.close();
        System.out.println(os==null?"null":os.toString());
        ftp.completePendingCommand();
        System.out.println("hha");

        ftp.logout();
       /* if(!ftp.completePendingCommand()) {
            ftp.logout();
            ftp.disconnect();
            System.err.println("File transfer failed.");
            System.exit(1);
        }*/
        /*if(ftp.isConnected()) {
            ftp.disconnect();
        }*/
    }
}
