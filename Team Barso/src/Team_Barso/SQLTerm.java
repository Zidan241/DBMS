package Team_Barso;

public class SQLTerm {
String _strTableName=""	;
String	_strColumnName="";
String	_strOperator="";
Object 	_objValue="";




public boolean equals(SQLTerm S1) {
	return this._strTableName.equals(S1._strTableName)&&this._strColumnName.equals(S1._strColumnName)&&this._strOperator.equals(S1._strOperator)&&this._objValue.equals(S1._objValue);
}


}
