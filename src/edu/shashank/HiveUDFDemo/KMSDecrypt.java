package edu.shashank.HiveUDFDemo;
import java.nio.ByteBuffer;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.util.Base64;

public final class KMSDecrypt extends GenericUDF {
	
	PrimitiveObjectInspector inputOI;
	PrimitiveObjectInspector outputOI;


	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		if(arg0.length!=1) return null;
		
		//get hive object
		Object oin = arg0[0].get();
		
		String cipherText = (String)inputOI.getPrimitiveJavaObject(oin);
		String plainText = decrypt(cipherText);
						
		return plainText;
	}

	public String decrypt(String cipherText) {
		AWSKMS kms = new AWSKMSClient();
		kms.setRegion(Region.getRegion(Regions.US_WEST_2));
		ByteBuffer cipherByteBuffer = ByteBuffer.wrap(Base64.decode(cipherText));
		DecryptRequest decryptRequest = new DecryptRequest().withCiphertextBlob(cipherByteBuffer);
		ByteBuffer plainTextByteBuffer = kms.decrypt(decryptRequest).getPlaintext();
		String plainText = new String(plainTextByteBuffer.array());
		return "<<<"+plainText+">>>";
		
	}

	@Override
	public String getDisplayString(String[] arg0) {
		//Any message
		return "fail";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		//Accepts only one argument, ie. base64 encoded encrypted cipher
		assert(arg0.length == 1);
		
		inputOI = (PrimitiveObjectInspector) arg0[0];
		outputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		return outputOI;
	}

}
