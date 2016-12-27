using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Common.tools {    //
    /// <summary>
    /// 存取資料庫工具類別
    /// </summary>
    public class DBTools {
        private string connString = null;
        public bool reqError = false;
        public string reqErrorText = null;

        public DBTools(string connString) {
            this.connString = connString;
        }

        #region *** ADO.NET Common ***

        /// <summary>
        /// 要求資料庫資料(DataTable)
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <returns>DataTable</returns>
        public DataTable requestDBToDataTable(string commandText) {
            return requestDBToDataTable(commandText, null);
        }

        /// <summary>
        /// 要求資料庫資料(DataTable)
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <param name="dicy">command參數</param>
        /// <returns>DataTable</returns>
        public DataTable requestDBToDataTable(string commandText, Dictionary<string, string> dicy) {
            this.reqError = false;
            this.reqErrorText = null;
            DataTable dataTable = new DataTable();
            SqlConnection sqlConnection = new SqlConnection(connString);
            SqlCommand sqlCommand = new SqlCommand(commandText, sqlConnection);
            if (dicy != null) { foreach (string key in dicy.Keys) { sqlCommand.Parameters.AddWithValue(key, dicy[key]); } }
            SqlDataAdapter adapter = new SqlDataAdapter(sqlCommand);

            try {
                if (sqlConnection.State == ConnectionState.Closed) { sqlConnection.Open(); }
                adapter.Fill(dataTable);
            } catch (Exception ex) {
                this.reqError = true;
                this.reqErrorText = ex.Message;
                throw ex;
            } finally {
                if (sqlCommand != null) { sqlCommand.Dispose(); }
                if (adapter != null) { adapter.Dispose(); }
                if (sqlConnection != null) {
                    sqlConnection.Close();
                    sqlConnection.Dispose();
                }
            }

            return dataTable;
        }

        /// <summary>
        /// 要求資料庫資料(DataSet)
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <returns>DataSet</returns>
        public DataSet requestDBToDataSet(string commandText) {
            return requestDBToDataSet(commandText, null);
        }

        /// <summary>
        /// 要求資料庫資料(DataSet)
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <param name="dicy">command參數</param>
        /// <returns>DataSet</returns>
        public DataSet requestDBToDataSet(string commandText, Dictionary<string, string> dicy) {
            this.reqError = false;
            this.reqErrorText = null;
            DataSet dataSet = new DataSet();
            SqlConnection sqlConnection = new SqlConnection(connString);
            SqlCommand sqlCommand = new SqlCommand(commandText, sqlConnection);
            if (dicy != null) { foreach (string key in dicy.Keys) { sqlCommand.Parameters.AddWithValue(key, dicy[key]); } }
            SqlDataAdapter adapter = new SqlDataAdapter(sqlCommand);

            try {
                if (sqlConnection.State == ConnectionState.Closed) { sqlConnection.Open(); }
                adapter.Fill(dataSet);
            } catch (Exception ex) {
                this.reqError = true;
                this.reqErrorText = ex.Message;
                throw ex;
            } finally {
                if (sqlCommand != null) { sqlCommand.Dispose(); }
                if (adapter != null) { adapter.Dispose(); }
                if (sqlConnection != null) {
                    sqlConnection.Close();
                    sqlConnection.Dispose();
                }
            }

            return dataSet;
        }

        /// <summary>
        /// 要求資料庫資料(DataReader，使用結束務必關閉 SqlDataReader 物件)
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <returns>DataReader</returns>
        public SqlDataReader requestDBToDataReader(string commandText) {
            return requestDBToDataReader(commandText, null);
        }

        /// <summary>
        /// 要求資料庫資料(DataReader，使用結束務必關閉 SqlDataReader 物件)
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <param name="dicy">command參數</param>
        /// <returns>DataReader</returns>
        public SqlDataReader requestDBToDataReader(string commandText, Dictionary<string, object> dicy) {
            this.reqError = false;
            this.reqErrorText = null;
            SqlDataReader resultReader = null;
            SqlConnection sqlConnection = new SqlConnection(connString);
            SqlCommand sqlCommand = new SqlCommand(commandText, sqlConnection);

            if (dicy != null) {
                foreach (string key in dicy.Keys) {
                    sqlCommand.Parameters.AddWithValue(key, dicy[key]);
                }
            }

            try {
                if (sqlConnection.State == ConnectionState.Closed) { sqlConnection.Open(); }
                resultReader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            } catch (Exception ex) {
                this.reqError = true;
                this.reqErrorText = ex.Message;

                if (resultReader != null) {
                    resultReader.Close();
                    resultReader.Dispose();
                }

                if (sqlCommand != null) {
                    sqlCommand.Dispose();
                }

                if (sqlConnection != null) {
                    sqlConnection.Close();
                    sqlConnection.Dispose();
                }

                throw ex;
            }

            return resultReader;
        }

        /// <summary>
        /// 要求資料庫第一筆資料
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <returns>object</returns>
        public object requestDBWithSingle(string commandText) {
            return requestDBWithSingle(commandText, null);
        }

        /// <summary>
        /// 要求資料庫第一筆資料
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <param name="dicy">command參數</param>
        /// <returns>object</returns>
        public object requestDBWithSingle(string commandText, Dictionary<string, object> dicy) {
            this.reqError = false;
            this.reqErrorText = null;
            object resultObject = null;

            using (SqlConnection sqlConnection = new SqlConnection(connString)) {
                using (SqlCommand sqlCommand = new SqlCommand(commandText, sqlConnection)) {

                    if (dicy != null) {
                        foreach (string key in dicy.Keys) {
                            sqlCommand.Parameters.AddWithValue(key, dicy[key]);
                        }
                    }

                    try {
                        if (sqlConnection.State == ConnectionState.Closed) { sqlConnection.Open(); }
                        resultObject = sqlCommand.ExecuteScalar();
                    } catch (Exception ex) {
                        this.reqError = true;
                        this.reqErrorText = ex.Message;
                        throw ex;
                    }
                }
            }

            return resultObject;
        }

        /// <summary>
        /// 異動資料庫資料
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <returns>int</returns>
        public int modifyDBData(string commandText) {
            return modifyDBData(commandText, null);
        }

        /// <summary>
        /// 異動資料庫資料
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <param name="dicy">command參數</param>
        /// <returns>int</returns>
        public int modifyDBData(string commandText, Dictionary<string, object> dicy) {
            return modifyDBData(commandText, dicy, IsolationLevel.ReadUncommitted);
        }

        /// <summary>
        /// 異動資料庫資料(自定義交易等級)
        /// </summary>
        /// <param name="commandText">command文字</param>
        /// <param name="dicy">command參數</param>
        /// <param name="isoLevel">交易等級</param>
        /// <returns>int</returns>
        public int modifyDBData(string commandText, Dictionary<string, object> dicy, IsolationLevel isoLevel) {
            this.reqError = false;
            this.reqErrorText = null;
            SqlConnection sqlConnection = new SqlConnection(connString);
            SqlCommand sqlCommand = new SqlCommand(commandText, sqlConnection);
            SqlTransaction transaction = null;
            if (dicy != null) { foreach (string key in dicy.Keys) { sqlCommand.Parameters.AddWithValue(key, dicy[key]); } }
            int result = -1;

            try {
                if (sqlConnection.State == ConnectionState.Closed) { sqlConnection.Open(); }
                transaction = sqlConnection.BeginTransaction(isoLevel);
                sqlCommand.Transaction = transaction;
                result = sqlCommand.ExecuteNonQuery();
                transaction.Commit();
            } catch (Exception ex) {
                transaction.Rollback();
                this.reqError = true;
                this.reqErrorText = ex.Message;
                throw ex;
            } finally {
                if (sqlCommand != null) { sqlCommand.Dispose(); }
                if (sqlConnection != null) {
                    sqlConnection.Close();
                    sqlConnection.Dispose();
                }
            }

            return result;
        }

        /// <summary>
        /// 存取資料庫預存程序
        /// </summary>
        /// <param name="spName">預存程序</param>
        /// <param name="sqlParams">預存程序參數陣列</param>
        public void useStoredProcedure(string spName) {
            useStoredProcedure(spName, null);
        }

        /// <summary>
        /// 存取資料庫預存程序
        /// </summary>
        /// <param name="spName">預存程序</param>
        /// <param name="sqlParams">預存程序參數陣列</param>
        public void useStoredProcedure(string spName, SqlParameter[] sqlParams) {
            this.reqError = false;
            this.reqErrorText = null;
            using (SqlConnection sqlConn = new SqlConnection(connString)) {
                using (SqlCommand sqlCmd = new SqlCommand(spName, sqlConn)) {
                    sqlCmd.CommandType = CommandType.StoredProcedure;

                    if (sqlParams != null) { sqlCmd.Parameters.AddRange(sqlParams); }

                    try {
                        if (sqlConn.State == ConnectionState.Closed) { sqlConn.Open(); }
                        sqlCmd.ExecuteNonQuery();
                    } catch (Exception ex) {
                        this.reqError = true;
                        this.reqErrorText = ex.Message;
                        throw ex;
                    }
                }
            }
        }

        /// <summary>
        /// 存取資料庫預存程序(回傳DataSet)
        /// </summary>
        /// <param name="spName">預存程序</param>
        /// <param name="sqlParams">預存程序參數陣列</param>
        /// <returns>DataSet</returns>
        public DataSet storedProcedureToDataSet(string spName) {
            return storedProcedureToDataSet(spName, null);
        }

        /// <summary>
        /// 存取資料庫預存程序(回傳DataSet)
        /// </summary>
        /// <param name="spName">預存程序</param>
        /// <param name="sqlParams">預存程序參數陣列</param>
        /// <returns>DataSet</returns>
        public DataSet storedProcedureToDataSet(string spName, SqlParameter[] sqlParams) {
            this.reqError = false;
            this.reqErrorText = null;
            DataSet responseDS = new DataSet();

            using (SqlConnection sqlConn = new SqlConnection(connString)) {
                using (SqlCommand sqlCmd = new SqlCommand(spName, sqlConn)) {
                    sqlCmd.CommandType = CommandType.StoredProcedure;

                    if (sqlParams != null) { sqlCmd.Parameters.AddRange(sqlParams); }

                    try {
                        if (sqlConn.State == ConnectionState.Closed) { sqlConn.Open(); }
                        using (SqlDataAdapter adapter = new SqlDataAdapter(sqlCmd)) {
                            adapter.Fill(responseDS);
                        }
                    } catch (Exception ex) {
                        this.reqError = true;
                        this.reqErrorText = ex.Message;
                        throw ex;
                    }
                }
            }

            return responseDS;
        }

        #endregion

        #region *** ADO.NET With Dapper ***

        #region *** 查詢單一表格資料模型清單 ***

        /// <summary>
        /// 使用參數查詢資料庫資料，並轉為實體動態型別模型 List 集合
        /// </summary>
        /// <param name="strCommand">T-SQL command</param>
        /// <param name="objParams">查詢參數物件</param>
        /// <param name="isoLevel">交易等級(預設為最寬鬆等級)</param>
        /// <returns>List<dynamic></returns>
        public List<dynamic> queryModelList(string strCommand, object objParams = null,
            IsolationLevel isoLevel = IsolationLevel.ReadUncommitted) {

            return queryModelList<dynamic>(strCommand, objParams, isoLevel);
        }

        /// <summary>
        /// 使用參數查詢資料庫資料，並轉為指定的實體類別模型 List 集合
        /// </summary>
        /// <param name="strCommand">T-SQL command</param>
        /// <param name="objParams">查詢參數物件</param>
        /// <param name="isoLevel">交易等級(預設為最寬鬆等級)</param>
        /// <returns>List<T></returns>
        public List<T> queryModelList<T>(string strCommand, object objParams = null,
            IsolationLevel isoLevel = IsolationLevel.ReadUncommitted) {

            this.reqError = false;
            this.reqErrorText = null;
            List<T> listModel;

            using (SqlConnection sqlConn = new SqlConnection(connString)) {
                sqlConn.Open();

                using (SqlTransaction trans = sqlConn.BeginTransaction(isoLevel)) {
                    try {
                        listModel = sqlConn.Query<T>(strCommand, objParams, trans).ToList();
                        trans.Commit();
                    } catch (Exception ex) {
                        trans.Rollback();
                        this.reqError = true;
                        this.reqErrorText = ex.Message;
                        throw ex;
                    }
                }
            }

            return listModel;
        }

        #endregion

        #region *** 查詢單一表格資料模型清單，並回傳第一筆資料 ***

        /// <summary>
        /// 要求資料庫第一筆資料，並轉為實體動態型別模型
        /// 呼叫函式時可指定回傳的實體類別模型型別，若不指定需傳入 dynamic 動態型別
        /// </summary>
        /// <param name="strCommand">T-SQL command</param>
        /// <param name="objParams">查詢參數物件</param>
        /// <param name="isoLevel">交易等級(預設為最寬鬆等級)</param>
        /// <returns>dynamic</returns>
        public dynamic querySingleModel(string strCommand, object objParams = null,
            IsolationLevel isoLevel = IsolationLevel.ReadUncommitted) {

            return querySingleModel<dynamic>(strCommand, objParams, isoLevel);
        }

        /// <summary>
        /// 使用參數查詢資料庫資料，並轉為指定的實體類別模型
        /// </summary>
        /// <param name="strCommand">T-SQL command</param>
        /// <param name="objParams">查詢參數物件</param>
        /// <param name="isoLevel">交易等級(預設為最寬鬆等級)</param>
        /// <returns>T</returns>
        public T querySingleModel<T>(string strCommand, object objParams = null,
            IsolationLevel isoLevel = IsolationLevel.ReadUncommitted) {

            this.reqError = false;
            this.reqErrorText = null;
            T model;

            using (SqlConnection sqlConn = new SqlConnection(connString)) {
                sqlConn.Open();

                using (SqlTransaction trans = sqlConn.BeginTransaction(isoLevel)) {
                    try {
                        model = sqlConn.Query<T>(strCommand, objParams, trans).FirstOrDefault();
                        trans.Commit();
                    } catch (Exception ex) {
                        trans.Rollback();
                        this.reqError = true;
                        this.reqErrorText = ex.Message;
                        throw ex;
                    }
                }
            }

            return model;
        }

        #endregion

        #region *** 查詢多個資料庫表格資料模型清單 ***

        /// <summary>
        /// 使用參數查詢多個資料庫表格資料，並轉為指定的實體型別模型 List 集合
        /// </summary>
        /// <param name="strCommand">T-SQL command</param>
        /// <param name="objParams">查詢參數物件</param>
        /// <param name="isoLevel">交易等級(預設為最寬鬆等級)</param>
        /// <param name="aryModelType">指定實體型別模型陣列</param>
        /// <returns>List<List<dynamic>></returns>
        public List<List<dynamic>> queryMultiTypeModel(string strCommand, object objParams = null,
            IsolationLevel isoLevel = IsolationLevel.ReadUncommitted, params Type[] aryModelType) {

            this.reqError = false;
            this.reqErrorText = null;
            List<List<dynamic>> listQueryModelList = new List<List<dynamic>>();

            using (SqlConnection sqlConn = new SqlConnection(connString)) {
                sqlConn.Open();

                using (SqlTransaction trans = sqlConn.BeginTransaction(isoLevel)) {
                    try {
                        using (var multiple = sqlConn.QueryMultiple(strCommand, objParams, trans)) {
                            if (aryModelType != null && aryModelType.Length > 0) {
                                aryModelType.ToList().ForEach(type => listQueryModelList.Add(multiple.Read(type).ToList()));
                            } else {
                                while (!multiple.IsConsumed) {
                                    listQueryModelList.Add(multiple.Read().ToList());
                                }
                            }

                            trans.Commit();
                        }
                    } catch (Exception ex) {
                        trans.Rollback();
                        this.reqError = true;
                        this.reqErrorText = ex.Message;
                        throw ex;
                    }
                }
            }

            return listQueryModelList;
        }

        #endregion

        #region *** 利用資料模型進行資料庫異動 ***

        /// <summary>
        /// 使用自訂物件模型進行資料庫單筆或多筆資料異動
        /// </summary>
        /// <param name="strCommand">T-SQL command</param>
        /// <param name="objParams">異動資料物件模型</param>
        /// <param name="isoLevel">交易等級(預設為最寬鬆等級)</param>
        /// <returns>int</returns>
        public int modifyDBDataWithModel(string strCommand, object objParams = null,
            IsolationLevel isoLevel = IsolationLevel.ReadUncommitted) {

            this.reqError = false;
            this.reqErrorText = null;
            int result = -1;

            using (SqlConnection sqlConn = new SqlConnection(connString)) {
                sqlConn.Open();

                using (SqlTransaction trans = sqlConn.BeginTransaction(isoLevel)) {
                    try {
                        result = sqlConn.Execute(strCommand, objParams, trans);
                        trans.Commit();
                    } catch (Exception ex) {
                        trans.Rollback();
                        this.reqError = true;
                        this.reqErrorText = ex.Message;
                        throw ex;
                    }
                }
            }

            return result;
        }

        #endregion

        #region *** 資料庫預存程序查詢 ***

        /// <summary>
        /// 查詢預存程序，並回傳動態型別模型 List 集合與輸出參數值
        /// </summary>
        /// <param name="spName">預存程序名稱</param>
        /// <param name="spParams">預存程序輸入輸出參數</param>
        /// <param name="isoLevel">交易等級(預設為最寬鬆等級)</param>
        /// <returns>List<dynamic></returns>
        public List<dynamic> queryStoredProcedure(string spName, DynamicParameters spParams = null,
            IsolationLevel isoLevel = IsolationLevel.ReadUncommitted) {

            return queryStoredProcedure<dynamic>(spName, spParams, isoLevel);
        }

        /// <summary>
        /// 查詢預存程序，並回傳指定的實體型別模型 List 集合與輸出參數值
        /// </summary>
        /// <param name="spName">預存程序名稱</param>
        /// <param name="spParams">預存程序輸入參數</param>
        /// <param name="isoLevel">交易等級(預設為最寬鬆等級)</param>
        /// <returns>List<T></returns>
        public List<T> queryStoredProcedure<T>(string spName, DynamicParameters spParams = null,
            IsolationLevel isoLevel = IsolationLevel.ReadUncommitted) {

            this.reqError = false;
            this.reqErrorText = null;
            List<T> listQuerySpResult;

            using (SqlConnection sqlConn = new SqlConnection(connString)) {
                sqlConn.Open();

                using (SqlTransaction trans = sqlConn.BeginTransaction(isoLevel)) {
                    try {
                        listQuerySpResult = sqlConn.Query<T>(spName, spParams, trans, commandType: CommandType.StoredProcedure).ToList();
                        trans.Commit();
                    } catch (Exception ex) {
                        trans.Rollback();
                        this.reqError = true;
                        this.reqErrorText = ex.Message;
                        throw ex;
                    }
                }
            }

            return listQuerySpResult;
        }

        /// <summary>
        /// 建立預存程序輸入輸出參數物件
        /// </summary>
        /// <returns></returns>
        public DynamicParameters createDbParams() {
            return new DynamicParameters();
        }

        #endregion

        #endregion

    }
}

