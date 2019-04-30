package FileUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class WriteOutputFile {
	
	public static void writeObjectInFile(String path, Object obj) throws FileNotFoundException, IOException {
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(path));
        objectOutputStream.writeObject(obj);
        objectOutputStream.close();
	}
	
	
	public static Object readObjectInFile(String path) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(path));
		Object object = objectInputStream.readObject();
	    objectInputStream.close();
		return object;
	}
}
