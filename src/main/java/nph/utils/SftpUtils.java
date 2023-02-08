package nph.utils;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class SftpUtils {

    public static void download(String source, String destination,
                                String sftpHost, String sftpUsername, String sftpPassword) throws IOException {
        SSHClient client = new SSHClient();
        SFTPClient sftpClient = getClient(client, sftpHost, sftpUsername, sftpPassword);
        sftpClient.get(source, destination);

        sftpClient.close();
        client.disconnect();
    }

    public static void upload(String source, String destination,
                                String sftpHost, String sftpUsername, String sftpPassword) throws IOException {
        SSHClient client = new SSHClient();
        SFTPClient sftpClient = getClient(client, sftpHost, sftpUsername, sftpPassword);
        sftpClient.put(source, destination);

        sftpClient.close();
        client.disconnect();
    }

    public static void upload(String source, String destination,
                              String sftpHost, int sftpPort,
                              String sftpUsername, String sftpPassword) throws IOException {
        SSHClient client = new SSHClient();
        SFTPClient sftpClient = getClient(client, sftpHost, sftpPort, sftpUsername, sftpPassword);
        sftpClient.put(source, destination);

        sftpClient.close();
        client.disconnect();
    }


    public static List<String> ls(String sourceDir, String sftpHost, String sftpUsername, String sftpPassword) throws IOException {
        SSHClient client = new SSHClient();
        SFTPClient sftpClient = getClient(client, sftpHost, sftpUsername, sftpPassword);

        List<String> result = sftpClient.ls(sourceDir).stream()
                .map(RemoteResourceInfo::getPath)
                .collect(Collectors.toList());
        sftpClient.close();
        client.disconnect();
        return result;
    }

    public static SFTPClient getClient(SSHClient client, String sftpHost, String sftpUsername, String sftpPassword) throws IOException {
        client.addHostKeyVerifier(new PromiscuousVerifier());
        client.connect(sftpHost);
        client.authPassword(sftpUsername, sftpPassword);

        return client.newSFTPClient();
    }

    public static SFTPClient getClient(SSHClient client, String sftpHost, int sftpPort, String sftpUsername, String sftpPassword) throws IOException {
        client.addHostKeyVerifier(new PromiscuousVerifier());
        client.connect(sftpHost, sftpPort);
        client.authPassword(sftpUsername, sftpPassword);

        return client.newSFTPClient();
    }
}
