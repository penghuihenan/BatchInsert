using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using Oracle.DataAccess.Client;
using System.Data;
using System.Diagnostics;

namespace DBBatchInsert
{
    /// <summary>
    /// MainWindow.xaml 的交互逻辑
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();

            InitType();

            InitTimes();
        }


        private int GetTimes()
        {
            return int.Parse(insertNumber.Text);
        }

        private InsertType GetInsertType()
        {
            return (InsertType)Enum.Parse(typeof(InsertType), insertType.Text);
        }

        private void InitTimes()
        {
            insertNumber.Items.Add(1000);
            insertNumber.Items.Add(10000);
            insertNumber.Items.Add(100000);
            insertNumber.Items.Add(1000000);
            insertNumber.SelectedIndex = 0;
        }

        private void InitType()
        {
            insertType.Items.Add(InsertType.普通);
            insertType.Items.Add(InsertType.Table批量);
            insertType.SelectedIndex = 0;
        }

        private OracleConnection GetConn()
        {
            OracleConnection conn = new OracleConnection("data source=(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST =10.2.167.84)(PORT = 1521)))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = orcl)) );user id=HXJT_BeiJing;password=sim2013");
            conn.Open();
            return conn;
        }

        String Msg = String.Empty;

        private void Insert()
        {
            Msg = String.Empty;
            txtMsg.Text = String.Empty;
            OracleConnection conn = GetConn();
            OracleCommand comm = new OracleCommand();
            comm.Connection = conn;
            comm.CommandType = CommandType.Text;

            try
            {
                Stopwatch st = Stopwatch.StartNew();
//                 st.Start();
//                 CommandInsert(comm);
//                 st.Stop();
//                 Msg += String.Format("普通插入用时:{0}秒\n", st.ElapsedMilliseconds / 1000);


                st.Start();
                BatchInsert2(comm);
                st.Stop();
                Msg += String.Format("普通插入用时:{0}秒\n", st.ElapsedMilliseconds / 1000);

//                 st.Start();
//                 BatchInsert(comm);
//                 st.Stop();
//                 Msg += String.Format("批量插入用时:{0}秒", st.ElapsedMilliseconds / 1000);

                txtMsg.Text = Msg;
            }
            catch (System.Exception ex)
            {
            }
            finally
            {
                conn.Close();
            }
        }

        private void BatchInsert2(OracleCommand comm)
        {
            Msg += "开始:" + DateTime.Now.ToString("HH:mm:ss fff") + "\n";
            DataTable dt = new DataTable();
            int count = GetTimes();
            dt.TableName = "AAA";
            dt.Columns.Add("EID", typeof(String));
            dt.Columns.Add("FROMID", typeof(String));
            dt.Columns.Add("TOID", typeof(String));
            dt.Columns.Add("LL", typeof(Int32));
            dt.Columns.Add("YW", typeof(Int32));
            dt.Columns.Add("LXSJ", typeof(Int32));
          

            Msg += "初始前:" + DateTime.Now.ToString("HH:mm:ss fff") + "\n";
            for (int i = 0; i < count; i++)
            {
                dt.Rows.Add(new Object[] { DateTime.Now.ToString("HH:mm:ss"), "1", "1", 1, 1, 1 });
            }
            Msg += "初始后:" + DateTime.Now.ToString("HH:mm:ss fff") + "\n";

            Msg += "转换前:" + DateTime.Now.ToString("HH:mm:ss fff") + "\n";
            Dictionary<string, object> datas = this.ConvertDataTableToDictionary(dt);
            Msg += "转换后:" + DateTime.Now.ToString("HH:mm:ss fff") + "\n";

            Msg += "提交前:" + DateTime.Now.ToString("HH:mm:ss fff") + "\n";
            BatchInsertOperate(comm.Connection, dt.TableName, datas, dt.Rows.Count);
            BatchInsertOperate(comm.Connection, dt.TableName, datas, dt.Rows.Count);
            Msg += "提交后:" + DateTime.Now.ToString("HH:mm:ss fff") + "\n";
        }

        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <param name="tableName">表名称 </param>
        /// <param name="columnRowData">键-值存储的批量数据：键是列名称，值是对应的数据集合</param>
        /// <param name="len">每次批处理数据的大小</param>
        /// <returns></returns>
        public int BatchInsertOperate(OracleConnection conn,String tableName, Dictionary<string, object> columnRowData, int len)
        {
            int iResult = 0;
            string[] dbColumns = columnRowData.Keys.ToArray();

            if (columnRowData.Count > 0)
            {
                String sql = String.Format("INSERT INTO {0} ({1}) values(:{2})", tableName, string.Join(",", dbColumns),string.Join(",:", dbColumns));

               
                using (OracleCommand cmd = conn.CreateCommand())
                    {
                        //绑定批处理的行数  
                        cmd.ArrayBindCount = len;
                        cmd.BindByName = true;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = sql;
                        cmd.CommandTimeout = 10*60;//10分钟  

                        //创建参数  
                        foreach (string colName in dbColumns)
                        {
                            OracleDbType dbType = GetOracleDbType(columnRowData[colName]);
                            OracleParameter  oraParam = new OracleParameter(colName, dbType,columnRowData[colName],ParameterDirection.Input);
                            cmd.Parameters.Add(oraParam);
                        }
                        /*执行批处理*/
                        var trans = conn.BeginTransaction();
                        try
                        {
                            cmd.Transaction = trans;
                            iResult = cmd.ExecuteNonQuery();
                            trans.Commit();
                        }
                        catch (Exception ex)
                        {
                            trans.Rollback();
                            throw ex;
                        }
                        finally
                        {
                        }
                    }
                }
            
            return iResult;
        }


       /// <summary>
        /// 根据数据类型获取OracleDbType
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private OracleDbType GetOracleDbType(object value)
        {
            OracleDbType dataType = OracleDbType.Object;
            if (value is string[])
            {
                dataType = OracleDbType.Varchar2;
            }
            else if (value is DateTime[])
            {
                dataType = OracleDbType.TimeStamp;
            }
            else if (value is int[] || value is short[])
            {
                dataType = OracleDbType.Int32;
            }
            else if (value is long[])
            {
                dataType = OracleDbType.Int64;
            }
            else if (value is decimal[] || value is double[] || value is float[])
            {
                dataType = OracleDbType.Decimal;
            }
            else if (value is Guid[])
            {
                dataType = OracleDbType.Varchar2;
            }
            else if (value is bool[] || value is Boolean[])
            {
                dataType = OracleDbType.Byte;
            }
            else if (value is byte[])
            {
                dataType = OracleDbType.Blob;
            }
            else if (value is char[])
            {
                dataType = OracleDbType.Char;
            }
            return dataType;
        }

        private Dictionary<string, object> ConvertDataTableToDictionary(DataTable dt)
        {
            int len = dt.Rows.Count;
            Dictionary<string, object> dataDic = new Dictionary<string, object>();
            String[] EID = new String[len];
            String[] FROMID = new String[len];
            String[] TOID = new String[len];
            decimal[] LL = new decimal[len];
            decimal[] YW = new decimal[len];
            decimal[] LXSJ = new decimal[len];
          

            for (int i = 0; i < len; i++)
            {
                DataRow dr = dt.Rows[i];
                EID[i] = dr["EID"].ToString();
                FROMID[i] = (dr["FROMID"]).ToString();
                TOID[i] = (dr["TOID"]).ToString();
                LL[i] = DataConverter.ToDecimal(dr["LL"]);
                YW[i] = DataConverter.ToDecimal(dr["YW"]);
                LXSJ[i] = DataConverter.ToDecimal(dr["LXSJ"]);
            }

            dataDic.Add("EID", EID);
            dataDic.Add("FROMID", FROMID);
            dataDic.Add("TOID", TOID);
            dataDic.Add("LL", LL);
            dataDic.Add("YW", YW);
            dataDic.Add("LXSJ", LXSJ);
            return dataDic;
        }

        /// <summary>
        /// 先存入DataTable中，再插入，实际不快，为什么
        /// </summary>
        /// <param name="comm"></param>
        private void BatchInsert(OracleCommand comm)
        {
            Msg += "初始化前:" + DateTime.Now.ToString("HH:mm:ss fff")+"\n";
            DataTable dt = new DataTable();
            int count = GetTimes();
            dt.TableName = "AAATest";
            dt.Columns.Add("EID", typeof(String));
            dt.Columns.Add("FROMID", typeof(String));
            dt.Columns.Add("TOID", typeof(String));
            dt.Columns.Add("LL", typeof(Int32));
            dt.Columns.Add("YW", typeof(Int32));
            dt.Columns.Add("LXSJ", typeof(Int32));

            Msg += "初始化中:" + DateTime.Now.ToString("HH:mm:ss fff") + "\n";

            OracleDataAdapter da = new OracleDataAdapter(comm);
            OracleCommandBuilder ocb = new OracleCommandBuilder(da);
            da.SelectCommand.CommandText = "SELECT * FROM AAA";
            da.InsertCommand = ocb.GetInsertCommand();

            Msg += "初始化后:" + DateTime.Now.ToString("HH:mm:ss fff")+"\n";

            for (int i = 0; i < count; i++)
            {
                dt.Rows.Add(new Object[] { DateTime.Now.ToString("HH:mm:ss fff"), "1", "1", 1, 1, 1 });
            }
               
            Msg += "提交前:"+DateTime.Now.ToString("HH:mm:ss fff")+"\n";
            da.Update(dt);
            Msg += "提交后:" + DateTime.Now.ToString("HH:mm:ss fff")+"\n";
           
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            Insert();
        }
    }

    public class DataConverter
    {
        public static int ToInt(object o, int defVal = 0)
        {
            int res = defVal;
            if (o != null)
                int.TryParse(o.ToString(), out res);
            return res;
        }

        public static float ToFloat(object o, float defVal = 0)
        {
            float res = defVal;
            if (o != null)
                float.TryParse(o.ToString(), out res);
            return res;
        }

        public static double ToDouble(object o, double defVal = 0)
        {
            double res = defVal;
            if (o != null)
                double.TryParse(o.ToString(), out res);
            return res;
        }

        public static decimal ToDecimal(object o, decimal defVal = 0)
        {
            decimal res = defVal;
            if (o != null)
                decimal.TryParse(o.ToString(), out res);
            return res;
        }


        public static DateTime ToDateTime(object o, DateTime defVal)
        {
            DateTime res = defVal;
            if (o != null)
                DateTime.TryParse(o.ToString(), out res);
            return res;
        }
    }

    public enum InsertType
    {
        普通 = 0,
        Table批量 = 1,
    }
}
