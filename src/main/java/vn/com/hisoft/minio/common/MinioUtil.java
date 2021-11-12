package vn.com.hisoft.minio.common;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.http.Method;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.web.multipart.MultipartFile;

public class MinioUtil {
  private final String name;
  private final String secret;
  private final String url;
  private final String bucketName;
  private final MinioClient minioClient;

  public MinioUtil(String name, String secret, String url, String bucketName) {
    this.name = name;
    this.secret = secret;
    this.url = url;
    this.bucketName = bucketName;
    this.minioClient = MinioClient.builder().endpoint(url).credentials(name, secret).build();
  }

  public String upload(MultipartFile file)
      throws ServerException, InsufficientDataException, ErrorResponseException, IOException,
          NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException,
          XmlParserException, InternalException {
    if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
      throw new RuntimeException("Bucket does not exist!");
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(file.getBytes());
    String genFileName = UUID.randomUUID().toString().replace("-", "");
    genFileName = genFileName + "." + FilenameUtils.getExtension(file.getOriginalFilename());
    String key =
        new SimpleDateFormat("yyyy/MM/dd")
                .format(Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
            + "/"
            + genFileName;

    minioClient.putObject(
        PutObjectArgs.builder().bucket(bucketName).object(key).stream(bais, bais.available(), -1)
            .build());
    bais.close();
    return key;
  }

  public String getUrlFileByKey(String key) {
    try {
      return minioClient.getPresignedObjectUrl(
          GetPresignedObjectUrlArgs.builder()
              .method(Method.GET)
              .bucket(bucketName)
              .object(key)
              .expiry(60 * 60 * 24)
              .build());
    } catch (Exception ex) {
      throw new RuntimeException("Failed to load image!", ex);
    }
  }

  public byte[] getFile(String key) {
    try {
      InputStream obj =
          minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(key).build());

      byte[] content = IOUtils.toByteArray(obj);
      obj.close();
      return content;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
}
