package edu.shashank.HiveUDFDemo;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.Base64;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.shashank.Constants.Constants;

public class KMSDemo {

	public static void uploadToS3() {
		AmazonS3 s3client = new AmazonS3Client();
		try {
			PutObjectRequest putobj = new PutObjectRequest(Constants.BUCKET_NAME, "staf3.json",
					Files.newInputStream(Paths.get("staff3.json")), null);
			s3client.putObject(putobj);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String encrypt(String x) {
		String keyId = Constants.AWS_KEY_ARN;
		AWSKMS kms = AWSKMSClientBuilder.defaultClient();
		ByteBuffer plaintext = ByteBuffer.wrap(x.getBytes());
		EncryptRequest req = new EncryptRequest().withKeyId(keyId).withPlaintext(plaintext);
		ByteBuffer ciphertext = kms.encrypt(req).getCiphertextBlob();
		String cipher = null;
		if(ciphertext.hasArray())
			cipher = Base64.encodeAsString(ciphertext.array());
		return cipher;

	}

	private static Staff createDummyObject() {

		Staff staff = new Staff();

		staff.setName("Shashank");
		staff.setAge(28);
		staff.setPosition(encrypt("Ninja"));
		staff.setSalary(new BigDecimal("$1000000"));

		List<String> skills = new ArrayList<>();
		skills.add("java");
		skills.add("python");

		staff.setSkills(skills);

		return staff;

	}

	public static int run() {
		ObjectMapper mapper = new ObjectMapper();
		Staff staffRaw = createDummyObject();
		try {
			mapper.writeValue(new File("staff3.json"), staffRaw);
			uploadToS3();
		} catch (Exception e) {

		}
		return 0;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		run();
	}

}
