using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace tw.net.ebc {
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
            foreach (string key in dicy.Keys) { sqlCommand.Parameters.AddWithValue(key, dicy[key]); }
            SqlDataAdapter adapter = new SqlDataAdapter(sqlCommand);

            try {
                if (sqlConnection.State == ConnectionState.Closed) { sqlConnection.Open(); }
                adapter.Fill(dataTable);
            } catch (Exception ex) {
                this.reqError = true;
                this.reqErrorText = ex.Message;
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
        /// <param name="dicy">command參數</param>
        /// <returns>DataSet</returns>
        public DataSet requestDBToDataSet(string commandText, Dictionary<string, string> dicy) {
            this.reqError = false;
            this.reqErrorText = null;
            DataSet dataSet = new DataSet();
            SqlConnection sqlConnection = new SqlConnection(connString);
            SqlCommand sqlCommand = new SqlCommand(commandText, sqlConnection);
            foreach (string key in dicy.Keys) { sqlCommand.Parameters.AddWithValue(key, dicy[key]); }
            SqlDataAdapter adapter = new SqlDataAdapter(sqlCommand);

            try {
                if (sqlConnection.State == ConnectionState.Closed) { sqlConnection.Open(); }
                adapter.Fill(dataSet);
            } catch (Exception ex) {
                this.reqError = true;
                this.reqErrorText = ex.Message;
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
                    }
                }
            }

            return resultObject;
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
            foreach (string key in dicy.Keys) { sqlCommand.Parameters.AddWithValue(key, dicy[key]); }
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
        public void useStoredProdure(string spName, SqlParameter[] sqlParams = null) {
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
        public DataSet storedProdureToDataSet(string spName, SqlParameter[] sqlParams = null) {
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
                    }
                }
            }

            return responseDS;
        }
    }
}

