using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Ssepan.Io;
using Ssepan.Utility;
using System.Diagnostics;
using System.Reflection;

namespace Ssepan.Transaction
{
    public class TransactionFolders
    {
        #region Declarations
        public const String DateFormat = @"yyyy-MM-dd";
        public const String DateFormatOut = "{0:" + DateFormat + "}";
        //public const String CultureID = "en-US";

        public enum StateEnum
        { 
            Pending,
            Working,
            Error,
            Completed
        }
        #endregion Declarations

        #region Constructors
        public TransactionFolders()
        { 
        }

        public TransactionFolders
        (
            String pendingPath,
            String workingPath,
            String completedPath,
            String errorPath,
            Boolean movePendingToWorking,
            Boolean renamePendingFileOnMoveOrCopy,
            Boolean restoreOnError,
            Boolean useDatedCompletedFolder,
            Boolean useDatedErrorFolder
        )
        {
            try
            {
                PendingPath = pendingPath;
                WorkingPath = workingPath;
                CompletedPath = completedPath;
                ErrorPath = errorPath;
                Move = movePendingToWorking;
                Rename = renamePendingFileOnMoveOrCopy;
                RestoreOnError = restoreOnError;
                UseDatedCompletedFolder = useDatedCompletedFolder;
                UseDatedErrorFolder = useDatedErrorFolder;
            }
            catch (Exception ex)
            {
                Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                    
                throw;
            }
        }
        #endregion Constructors

        #region Properties
        #region Dependent Properties
        /// <summary>
        /// Name portion of transaction filename.
        /// </summary>
        public String ID
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.GetFileNameWithoutExtension(TransactionFilename); 
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Extension portion of transaction filename.
        /// </summary>
        public String Extension
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.GetExtension(TransactionFilename);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Extension portion of temporary filename.
        /// Returns a temporary extenstion that is different than the transaction file extension.
        /// </summary>
        public String TempExtension
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    //use single GUID
                    returnValue = String.Format(".{0}", InstanceID.ToString());
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Temporary filename (name and extension).
        /// Composed of ID + TempExtension.
        /// </summary>
        public String TempFilename
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = String.Format("{0}{1}", ID, TempExtension);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            } 
        }

        /// <summary>
        /// Full path to transaction file in pending folder.
        /// Composed of PendingPath + TransactionFilename.
        /// </summary>
        public String PendingFile
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(PendingPath, TransactionFilename);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Full path to temp-named transaction file in pending folder.
        /// Composed of PendingPath + TempFilename.
        /// </summary>
        public String PendingFileTemp
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(PendingPath, TempFilename);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Path of transaction id subfolder under working folder.
        /// Composed of WorkingPath + ID.
        /// </summary>
        public String WorkingFilePath
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(WorkingPath, ID);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Full path to transaction file in working folder.
        /// Composed of WorkingFilePath + TransactionFilename.
        /// </summary>
        public String WorkingFile
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(WorkingFilePath, TransactionFilename);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Full path to temp-named transaction file in working folder.
        /// Composed of WorkingFilePath + TempFilename.
        /// </summary>
        public String WorkingFileTemp
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(WorkingFilePath, TempFilename);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Dated subfolder of Completed folder.
        /// Composed of CompletedPath + DatedSubFolder.
        /// </summary>
        public String CompletedDatedPath
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(CompletedPath, DatedSubFolder);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Full path to transaction file in dated subfolder of Completed folder.
        /// Composed of CompletedPath + TransactionFilename.
        /// </summary>
        public String CompletedFilePath
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(CompletedPath, TransactionFilename); 
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Full path to transaction file in dated subfolder of Completed folder.
        /// Composed of CompletedDatedPath + TransactionFilename.
        /// </summary>
        public String CompletedDatedFilePath
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(CompletedDatedPath, TransactionFilename); 
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Dated subfolder of Error folder.
        /// Composed of ErrorPath + DatedSubFolder.
        /// </summary>
        public String ErrorDatedPath
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(ErrorPath, DatedSubFolder);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }

        /// <summary>
        /// Full path to transaction file in dated subfolder of Error folder.
        /// Composed of ErrorPath + TransactionFilename.
        /// </summary>
        public String ErrorFilePath
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(ErrorPath, TransactionFilename);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }
        
        /// <summary>
        /// Full path to transaction file in dated subfolder of Error folder.
        /// Composed of ErrorDatedPath + TransactionFilename.
        /// </summary>
        public String ErrorDatedFilePath
        {
            get 
            {
                String returnValue = default(String);
                try
                {
                    returnValue = Path.Combine(ErrorDatedPath, TransactionFilename);
                }
                catch (Exception ex)
                {
                    Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                    throw;
                }
                return returnValue;
            }
        }
        #endregion Dependent Properties

        #region Fixed Properties
        private Guid _InstanceID = Guid.NewGuid();
        public Guid InstanceID
        {
            get { return _InstanceID; }
            //set { _InstanceID = value; }
        }

        private StateEnum _State = default(StateEnum);
        public StateEnum State
        {
            get { return _State; }
            set { _State = value; }
        }

        private String _TransactionFilename = default(String);
        /// <summary>
        /// Filename, with extension and without path, of trasnaction file. Set from path to Pending file by Begin().
        /// </summary>
        public String TransactionFilename
        {
            get { return _TransactionFilename; }
            set { _TransactionFilename = value; }
        }

        private String _PendingPath = default(String);
        public String PendingPath
        {
            get { return _PendingPath; }
            set { _PendingPath = value; }
        }

        private String _WorkingPath = default(String);
        public String WorkingPath
        {
            get { return _WorkingPath; }
            set { _WorkingPath = value; }
        }

        private String _CompletedPath = default(String);
        public String CompletedPath
        {
            get { return _CompletedPath; }
            set { _CompletedPath = value; }
        }

        private String _ErrorPath = default(String);
        public String ErrorPath
        {
            get { return _ErrorPath; }
            set { _ErrorPath = value; }
        }

        private Boolean _Move = default(Boolean);
        /// <summary>
        /// Move file from Pending, instead of copying.
        /// The means by which processed fiels are distinguished from un-processed file are outside the scope of this class;
        ///  if the files must remain where they are and the caller wants to use another means of tracking processed files,
        ///  then set Move=false.
        /// </summary>
        public Boolean Move
        {
            get { return _Move; }
            set { _Move = value; }
        }

        private Boolean _Rename = default(Boolean);
        /// <summary>
        /// Use renaming of the file extension as a way to lock a file.
        /// While more than one process may, only one is likely to succeed;
        ///  a simplistic way to allow more than one process to run at a time.
        /// </summary>
        public Boolean Rename
        {
            get { return _Rename; }
            set { _Rename = value; }
        }

        private Boolean _RestoreOnError = default(Boolean);
        /// <summary>
        /// When a failed transaction should be automtically returned to the Pending folder, 
        ///  set RestoreOnError=true.
        /// </summary>
        public Boolean RestoreOnError
        {
            get { return _RestoreOnError; }
            set { _RestoreOnError = value; }
        }

        private Boolean _UseDatedCompletedFolder = default(Boolean);
        /// <summary>
        /// Use dated sub folders under Completed folder.
        /// </summary>
        public Boolean UseDatedCompletedFolder
        {
            get { return _UseDatedCompletedFolder; }
            set { _UseDatedCompletedFolder = value; }
        }

        private Boolean _UseDatedErrorFolder = default(Boolean);
        /// <summary>
        /// Use dated sub folders under Error folder.
        /// </summary>
        public Boolean UseDatedErrorFolder
        {
            get { return _UseDatedErrorFolder; }
            set { _UseDatedErrorFolder = value; }
        }

        private String _DatedSubFolder = GetDatedSubFolderNameFromDate(DateTime.Now); //String.Format(DateFormatOut, DateTime.Now);
        /// <summary>
        /// Subfolder name is Date formatted as string.
        /// </summary>
        public String DatedSubFolder
        {
            get { return _DatedSubFolder; }
            //set { _DatedSubFolder = value; }
        }
        #endregion Fixed Properties
        #endregion Properties

        #region Methods
        /// <summary>
        /// Validate parameters passed in constructor and used for all dependent properties.
        /// </summary>
        /// <returns></returns>
        private Boolean ValidateParameters()
        {
            Boolean returnValue = default(Boolean);
            try
            {
                if (TransactionFilename == null || TransactionFilename == String.Empty)
                {
                    throw new ArgumentException(String.Format("TransactionFilename is invalid: {0}", TransactionFilename));
                }
                if (ID == null || ID == String.Empty)
                {
                    throw new ArgumentException(String.Format("ID is invalid: {0}", ID));
                }
                if (Extension == null || Extension == String.Empty)
                {
                    throw new ArgumentException(String.Format("Extension is invalid: {0}", Extension));
                }

                if (PendingPath == null || PendingPath == String.Empty)
                {
                    throw new ArgumentException(String.Format("PendingPath is invalid: {0}", PendingPath));
                }
                if (!Directory.Exists(PendingPath))
                {
                    throw new ArgumentException(String.Format("PendingPath does not exist: {0}", PendingPath));
                }

                if (WorkingPath == null || WorkingPath == String.Empty)
                {
                    throw new ArgumentException(String.Format("WorkingPath is invalid: {0}", WorkingPath));
                }
                if (!Directory.Exists(WorkingPath))
                {
                    throw new ArgumentException(String.Format("WorkingPath does not exist: {0}", WorkingPath));
                }

                if (CompletedPath == null || CompletedPath == String.Empty)
                {
                    throw new ArgumentException(String.Format("CompletedPath is invalid: {0}", CompletedPath));
                }
                if (!Directory.Exists(CompletedPath))
                {
                    throw new ArgumentException(String.Format("CompletedPath does not exist: {0}", CompletedPath));
                }

                if (ErrorPath == null || ErrorPath == String.Empty)
                {
                    throw new ArgumentException(String.Format("ErrorPath is invalid: {0}", ErrorPath));
                }
                if (!Directory.Exists(ErrorPath))
                {
                    throw new ArgumentException(String.Format("ErrorPath does not exist: {0}", ErrorPath));
                }

                returnValue = true;
            }
            catch (Exception ex)
            {
                Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                    
            }
            return returnValue;
        }

        /// <summary>
        /// Begin a transaction. Copies or moves file from pending to working.
        /// </summary>
        /// <returns>Boolean</returns>
        /// <param name="errorMessage"></param>
        public Boolean Begin
        (
            String transactionFilePath,
            ref String errorMessage
        )
        {
            Boolean returnValue = default(Boolean);

            try
            {
                State = StateEnum.Pending;
                
                TransactionFilename = String.Format("{0}{1}", Path.GetFileNameWithoutExtension(transactionFilePath), Path.GetExtension(transactionFilePath));

                if (!ValidateParameters())
                {
                    throw new ArgumentException("One or more required parameters are not set correctly.");
                }

                if (!System.IO.File.Exists(PendingFile))
                {
                    throw new Exception(String.Format("PendingFile does not exist: {0}", PendingFile));
                }

                //create destination folder for transaction under Working 
                if (Directory.Exists(WorkingFilePath))
                {
                    throw new Exception(String.Format("WorkingFilePath already exists: {0}", WorkingFilePath));
                }
                Directory.CreateDirectory(WorkingFilePath);

                if (Rename)
                {
                    //RENAME
                    System.IO.File.Move(PendingFile, PendingFileTemp);
                    //TODO:consider delegate(s) as Func<String, Boolean> for file/directory exists to match copy/move
                    if (!System.IO.File.Exists(PendingFileTemp))
                    {
                        throw new Exception(String.Format("PendingFileTemp does not exist after rename: {0}", PendingFileTemp));
                    }

                    //TRANSFER
                    if (Move)
                    {//TODO:pass in move/copy delegate(s) as Action<String, String>, so that windows file system actions can be replaced with FTP actions, or perhaps even xfers via WCF service
                        System.IO.File.Move(PendingFileTemp, WorkingFileTemp);
                    }
                    else
                    {
                        System.IO.File.Copy(PendingFileTemp, WorkingFileTemp);
                        //Note:Handle renamed file left in folder after copy by performing rename back in End.
                    }
                    if (!System.IO.File.Exists(WorkingFileTemp))
                    {
                        throw new Exception(String.Format("WorkingFileTemp does not exist after Copy or Move: {0}", WorkingFileTemp));
                    }

                    //RENAME BACK
                    //Note:perform rename back (destination only - if source renamed and copy done, leave until End)
                    System.IO.File.Move(WorkingFileTemp, WorkingFile);
                    if (!System.IO.File.Exists(WorkingFile))
                    {
                        throw new Exception(String.Format("WorkingFile does not exist after rename: {0}", WorkingFile));
                    }
                }
                else
                {
                    //TRANSFER
                    if (Move)
                    {//TODO:pass in move/copy delegate(s) as Action<String, String>, so that windows file system actions can be replaced with FTP actions, or perhaps even xfers via WCF service
                        System.IO.File.Move(PendingFile, WorkingFile);
                    }
                    else
                    {
                        System.IO.File.Copy(PendingFile, WorkingFile);
                    }
                    if (!System.IO.File.Exists(WorkingFile))
                    {
                        throw new Exception(String.Format("WorkingFile does not exist: {0}", WorkingFile));
                    }
                }

                State = StateEnum.Working;

                returnValue = true;
            }
            catch (Exception ex)
            {
                Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                errorMessage = ex.Message;
            }
            return returnValue;
        }

        /// <summary>
        /// End a transaction. Copies file to completed or error. Deletes working folder.
        /// </summary>
        /// <returns>Boolean</returns>
        /// <param name="completed"></param>
        /// <param name="errorMessage"></param>
        public Boolean End
        (
            Boolean completed,
            ref String errorMessage
        )
        {
            Boolean returnValue = default(Boolean);
            String moveDestinationPath = String.Empty;       //path of file in completed or error folder

            try
            {
                if (!ValidateParameters())
                {
                    throw new ArgumentException("One or more required parameters are not set correctly.");
                }
                if (!System.IO.File.Exists(WorkingFile))
                {
                    throw new Exception(String.Format("WorkingFile does not exist: {0}", WorkingFile));
                }

                if (completed)
                {
                    if (UseDatedCompletedFolder)
                    {
                        //dated subdirectory may not exist yet
                        if (!Directory.Exists(CompletedDatedPath))
                        {
                            Directory.CreateDirectory(CompletedDatedPath);
                        }
                        System.IO.File.Copy(WorkingFile, CompletedDatedFilePath, true);
                        if (!System.IO.File.Exists(CompletedDatedFilePath))
                        {
                            throw new Exception(String.Format("CompletedDatedFilePath does not exist: {0}", CompletedDatedFilePath));
                        }
                    }
                    else
                    {
                        System.IO.File.Copy(WorkingFile, CompletedFilePath, true);
                        if (!System.IO.File.Exists(CompletedFilePath))
                        {
                            throw new Exception(String.Format("CompletedFilePath does not exist: {0}", CompletedFilePath));
                        }
                    }

                    //clean up working folder
                    Folder.DeleteFolderWithWait(WorkingFilePath, 1000);

                    State = StateEnum.Completed;
                }
                else //Error
                {
                    //not completed (error)
                    if (UseDatedErrorFolder)
                    {
                        //dated subdirectory may not exist yet
                        if (!Directory.Exists(ErrorDatedPath))
                        {
                            Directory.CreateDirectory(ErrorDatedPath);
                        }
                        System.IO.File.Copy(WorkingFile, ErrorDatedFilePath, true);
                        if (!System.IO.File.Exists(ErrorDatedFilePath))
                        {
                            throw new Exception(String.Format("ErrorDatedFilePath does not exist: {0}", ErrorDatedFilePath));
                        }
                    }
                    else
                    { 
                        System.IO.File.Copy(WorkingFile, ErrorFilePath, true);
                        if (!System.IO.File.Exists(ErrorFilePath))
                        {
                            throw new Exception(String.Format("ErrorFilePath does not exist: {0}", ErrorFilePath));
                        }
                    }

                    //restore error file to pending
                    if (RestoreOnError)
                    {
                        //Note: only if moved, not if copied.
                        if (Move)
                        {
                            System.IO.File.Copy(WorkingFile, PendingFile);
                            if (!System.IO.File.Exists(PendingFile))
                            {
                                throw new Exception(String.Format("PendingFile does not exist after restore: {0}", PendingFile));
                            }
                        }
                        else //Copy
                        {
                            //if rename
                            //    leave for rename logic below
                            //else
                            //    no need to copy back
                        }
                    }

                    //clean up working folder
                    Folder.DeleteFolderWithWait(WorkingFilePath, 1000);

                    State = StateEnum.Error;
                }

                if (Rename)
                {
                    if (!Move) //Copy
                    { 
                        //If renamed, Rename back.
                        if (!System.IO.File.Exists(PendingFileTemp))
                        {
                            throw new Exception(String.Format("PendingFileTemp does not exist before rename back: {0}", PendingFileTemp));
                        }
                        System.IO.File.Move(PendingFileTemp, PendingFile);
                        if (!System.IO.File.Exists(PendingFile))
                        {
                            throw new Exception(String.Format("PendingFile does not exist after rename back: {0}", PendingFile));
                        }
                    }
                }

                returnValue = true;
            }
            catch (Exception ex)
            {
                Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
                errorMessage = ex.Message;
            }
            return returnValue;
        }

        /// <summary>
        /// Clean up archives by removing dated folders (or files in undated folders) older than the specified duration from Completed or Error folders.
        /// Durations will be ignored if null. 
        /// UseDatedXxxFolder flags determine whether to delete dated folders or files in undated folders.
        /// </summary>
        /// <param name="completedDuration">Pass TimeSpan for duration, or null to retain indefinitely.</param>
        /// <param name="errorDuration">Pass TimeSpan for duration, or null to retain indefinitely.</param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public Boolean CleanUp
        (
            TimeSpan? completedDuration,
            TimeSpan? errorDuration,
            ref String errorMessage
        )
        {
            Boolean returnValue = default(Boolean);
            List<String> paths = default(List<String>);
            String folderName = default(String);
            DateTime FolderNameDate = default(DateTime);

            try
            {
                if (completedDuration != null)
                {
                    if (UseDatedCompletedFolder)
                    {
                        //Get list of dated subfolders under Completed.
                        paths = Directory.GetDirectories(CompletedPath, "*").ToList<String>();
                        //iterate dated folders
                        foreach (String folderPath in paths)
                        {
                            folderName = Path.GetFileName(folderPath);
                            if (!DateTime.TryParseExact(folderName, DateFormat, new CultureInfo(CultureInfo.CurrentCulture.Name, true), DateTimeStyles.None, out FolderNameDate))
                            {
                                throw new FormatException(String.Format("Unable to parse folder '{0}' in path '{1}' with format '{2}'.", folderName, folderPath, DateFormat));
                            }

                            if (DateTime.Now > (FolderNameDate + completedDuration))
                            {
                                Directory.Delete(folderPath, true);
                            }
                        }
                    }
                    else
                    {
                        //Extension is null if no file found to process
                        if ((Extension == null) || (Extension == String.Empty))
                        {
                            throw new ArgumentException(String.Format("Extension was not set and UseDatedCompletedFolder was set to '{0}'.", UseDatedCompletedFolder));
                        }

                        //Get list of transaction files in un-dated folder Completed.
                        paths = Directory.GetFiles(CompletedPath, String.Format("*{0}", Extension)).ToList<String>();
                        foreach (String filePath in paths)
                        {
                            DateTime fileDate = System.IO.File.GetLastWriteTime(filePath);
                            if (DateTime.Now > (fileDate + completedDuration))
                            {
                                System.IO.File.Delete(filePath);
                            }
                        }
                    }
                }

                if (errorDuration != null)
                {
                    if (UseDatedErrorFolder)
                    {
                        //Get list of dated subfolders under Error.
                        paths = Directory.GetDirectories(ErrorPath, "*").ToList<String>();
                        //iterate dated folders
                        foreach (String folderPath in paths)
                        {
                            folderName = Path.GetFileName(folderPath);
                            if (!DateTime.TryParseExact(folderName, DateFormat, new CultureInfo(CultureInfo.CurrentCulture.Name, true), DateTimeStyles.None, out FolderNameDate))
                            {
                                throw new FormatException(String.Format("Unable to parse folder '{0}' in path '{1}' with format '{2}'.", folderName, folderPath, DateFormat));
                            }

                            if (DateTime.Now > (FolderNameDate + errorDuration))
                            {
                                Directory.Delete(folderPath, true);
                            }
                        }
                    }
                    else
                    {
                        //Extension is null if no file found to process
                        if ((Extension == null) || (Extension == String.Empty))
                        {
                            throw new ArgumentException(String.Format("Extension was not set and UseDatedErrorFolder was set to '{0}'.", UseDatedErrorFolder));
                        }

                        //Get list of transaction files in un-dated folder Error.
                        paths = Directory.GetFiles(ErrorPath, String.Format("*{0}", Extension)).ToList<String>();
                        foreach (String filePath in paths)
                        {
                            DateTime fileDate = System.IO.File.GetLastWriteTime(filePath);
                            if (DateTime.Now > (fileDate + errorDuration))
                            {
                                System.IO.File.Delete(filePath);
                            }
                        }
                    }
                }

                returnValue = true;
            }
            catch (Exception ex)
            {
                Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                    
                errorMessage = ex.Message;
            }
            return returnValue;
        }

        #region Static Methods
        public static String GetDatedSubFolderNameFromDate(DateTime date)
        {
            String returnValue = default(String);

            try
            {
                returnValue = String.Format(DateFormatOut, date);
            }
            catch (Exception ex)
            {
                Log.Write(ex, MethodBase.GetCurrentMethod(), EventLogEntryType.Error);
                        
            }
            return returnValue;
        }
        #endregion Static Methods
        #endregion Methods
    }
}
