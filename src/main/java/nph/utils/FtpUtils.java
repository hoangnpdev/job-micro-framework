package nph.utils;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;


public class FtpUtils {
    public static Logger log = Logger.getLogger(FtpUtils.class);

    public static void download(String source, String destination,
                                String sftpHost, String sftpUsername, String sftpPassword) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public static void upload(String source, String destination,
                              String sftpHost, String sftpUsername, String sftpPassword) throws IOException {
        FTPClient ftpClient = getClient(sftpHost, sftpUsername, sftpPassword);
        FileInputStream fis = new FileInputStream(source);
        ftpClient.storeFile(destination, fis);
        fis.close();
        ftpClient.disconnect();
    }


    public static FTPClient getClient(String ftpHost, String ftpUsername, String ftpPassword) throws IOException {
        FTPClient ftpClient = new FTPClient();
        ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));
        ftpClient.connect(ftpHost, 21);
        int reply = ftpClient.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            ftpClient.disconnect();
            throw new IOException("Exception in connecting to sftp server. reply: " + reply);
        }
        ftpClient.login(ftpUsername, ftpPassword);
        return ftpClient;
    }
}
