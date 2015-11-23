package app;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

public class Demo1 {
	public static void main(String[] args) throws Exception {
		int a = -10;
		System.out.println(Arrays.toString(intToByteArray1(a)));
		System.out.println(Arrays.toString(intToByteArray2(a)));
		System.out.println(byteArrayToInt(new byte[]{-1, -1, -1, -10}));
		System.out.println(byteArrayToInt2(new byte[]{-1, -1, -1, -10}));
		System.out.println(Integer.toBinaryString(-42));
		System.out.println(Integer.toUnsignedLong(a));
		byte bytes = -42; 
		
		
		 int result = bytes&0xffffffff; 
		 System.out.println("无符号数: \t"+result); 
		 System.out.println("无符号数: \t"+(int)(bytes)); 
		 System.out.println("2进制bit位: \t"+Integer.toBinaryString(result)); 
	}

	

	public static byte[] intToByteArray1(int i) {
		byte[] result = new byte[4];
		result[0] = (byte) ((i >> 24) & 0xFF);
		result[1] = (byte) ((i >> 16) & 0xFF);
		result[2] = (byte) ((i >> 8) & 0xFF);
		result[3] = (byte) (i & 0xFF);
		return result;
	}

	public static byte[] intToByteArray2(int i) throws Exception {
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buf);
		out.writeInt(i);
		byte[] b = buf.toByteArray();
		out.close();
		buf.close();
		return b;
	}
	
	public static int byteArrayToInt(byte[] b){
		int i=0;   
        byte b1=(byte) ((b[0]&0xff)<<24);   
        byte b2=(byte) ((b[1]&0xff)<<16);   
        byte b3=(byte) ((b[2]&0xff)<<8);   
        byte b4=(byte) ((b[3]&0xff));   
        System.out.println(i);
		return i;   

			

	}
	
	
	public static int byteArrayToInt2(byte[] b){
		
        int b1=b[0]&0xFF<<24;  
        int b2=b[1]&0xFF<<16;   
        int b3=b[2]&0xFF<<8;   
        int b4=b[3]&0xFF;   
        
		return b1|b2|b3|b4;   

			

	}
}
