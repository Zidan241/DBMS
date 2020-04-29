package Team_Barso;

import java.io.IOException;
import java.lang.invoke.SerializedLambda;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;
import java.util.Vector;

public class test {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws IOException, DBAppException {
		SQLTerm S=new SQLTerm();
		S._objValue=4;
		S._strColumnName="testt";
		S._strOperator="<";
		S._strTableName="Stuednt";
		
		SQLTerm S1=new SQLTerm();
		S1._objValue=4;
		S1._strColumnName="tesatt";
		S1._strOperator="<";
		S1._strTableName="Stuednt";
		
		System.out.println(S.equals(S1));
		
		
	}

}
