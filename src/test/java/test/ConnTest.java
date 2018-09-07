package test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.gson.Gson;

/**
 * 根据官网文档搭建的简单demo。
 * @Date 2018年9月7日
 * @author E.E.
 *
 */
public class ConnTest {
	public static final String accessKey = "UEI2K7DZ9S0HC0UJ2Z63";
	public static final String secretKey = "DQgb4vbguTvxme2IAoFBOeaHorjFltvGO71UVYDT";
	public static final String endPoint = "http://mon1:7480";
	
	
	private static AmazonS3 getConn(){
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTP);

		AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
		conn.setEndpoint(endPoint);
		
		//域名访问，需要如下设置
		conn.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
		return conn;
	}
	
	/**
	 * 列出所有桶而已
	 * @param conn
	 */
	private static void listBuckets(AmazonS3 conn){
		List<Bucket> buckets = conn.listBuckets();
		for (Bucket bucket : buckets) {
			System.err.println(new Gson().toJson(bucket));
		}
	}
	
	private static void listBucketData(AmazonS3 conn, Bucket bucket){
		ObjectListing objects = conn.listObjects(bucket.getName());
		do {
	        for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
	        	System.err.println(new Gson().toJson(objectSummary));
	        }
	        objects = conn.listNextBatchOfObjects(objects);
		} while (objects.isTruncated());
	}
	
	
	private static void dataTest(AmazonS3 conn, Bucket bucket,String fileName){
		ByteArrayInputStream input = new ByteArrayInputStream("Hello World!".getBytes());
		conn.putObject(bucket.getName(), fileName, input, new ObjectMetadata());
		
		//权限控制 CannedAccessControlList.Private
		conn.setObjectAcl(bucket.getName(), fileName, CannedAccessControlList.PublicRead);
		//输出文件到E盘
		conn.getObject(
		        new GetObjectRequest(bucket.getName(), fileName),
		        new File("E:/"+fileName)
		);
		
		//下载链接
		GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket.getName(), fileName);
		System.err.println(conn.generatePresignedUrl(request));
	}
	
	public static void main(String[] args) {
		//创建连接
		AmazonS3 conn = getConn();
		
		//列出所有的bucket
		listBuckets(conn);
		
		//新建bucket
		Bucket bucket = conn.createBucket("my-new-bucket"+UUID.randomUUID().toString());

		String fileName = "hello.txt";
		
		//转为ByteArrayInputStream的方式保存文件
		//生成下载链接
		//下载到本地某个地方
		dataTest(conn, bucket,fileName);
		
		//列出这个bucket的所有数据
		listBucketData(conn, bucket);
		
		//删除文件（对象）
		//conn.deleteObject(bucket.getName(), fileName);
		
		//删除整个bucket
		//conn.deleteBucket(bucket.getName());
		
	}

}
