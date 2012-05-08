/****************************************************************************************
Filename: UPSSchema.sql
Type: Schema Create
Description: This will create the current version of the UPS objects.

Copyright 2010 RaneyDomain, LLC

Change History:
Date         Programmer         Change Made
------------ ------------------ --------------------------------------------------------
07-APR-2010  Kevin R. Raney     Initial Creation
14-APR-2010  Kevin R. Raney     Added email notifications for retries added to the queue in 
                                usp_USPManageQueue
16-APR-2010  Kevin R. Raney     Added usp_UPSWebGetPackageStatus for web application. 
21-APR-2010  Kevin R. Raney     Disabled email notifications. 
Unknown      Srikanth Pasunuri  Added PackageChannel to UPSPackage.
20-AUG-2010  Kevin R. Raney     Modified all foreign keys to cascade on delete. Added sprocs
                                usp_UPSSrcDestChange and usp_UPSAddPackageToQueue to support
                                the intraday automation.
****************************************************************************************/
use UPS_Agent;

create table UPSPackage (
  PackageID int identity(1,1) not null,
  PackageDesc varchar(150) not null,
  PackageOwnerEmail varchar(300) not null,
  PackageRetryAttempts int not null default 0,
  PackageRetryAgainInMins int not null default 0,
  PackageCreateDate datetime not null default getdate(),
  PackageChannel varchar(50),
  constraint UPSPackage_pk primary key (PackageID));
  
create table UPSSchedule (
  ScheduleID int identity(1,1) not null,
  SchedulePackageID int not null,
  ScheduleType varchar(100) not null, --Daily, Weekly, Monthly, MonthlyNthDOW
  ScheduleInterval int not null default 1, 
  ScheduleStartTime varchar(5) not null default '00:00',
  ScheduleEndTime varchar(5) not null default '23:59',
  ScheduleRepeatAfterMins int,
  ScheduleWeekSunFlag varchar(1),
  ScheduleWeekMonFlag varchar(1),
  ScheduleWeekTueFlag varchar(1),
  ScheduleWeekWedFlag varchar(1),
  ScheduleWeekThuFlag varchar(1),
  ScheduleWeekFriFlag varchar(1),
  ScheduleWeekSatFlag varchar(1),
  ScheduleMonthDOM int,
  ScheduleMonthNthOccurance int,
  ScheduleMonthNthDOW varchar(3),
  ScheduleStartDate datetime not null default getdate(),
  ScheduleEndDate datetime not null default '12/31/4750',
  constraint UPSSchedule_pk primary key (ScheduleID),
  constraint UPSSchedulePackage_fk foreign key (SchedulePackageID) references UPSPackage(PackageID) on delete cascade,
  constraint UPSScheduleType_ck check (ScheduleType in ('Daily','Weekly','Monthly','MonthlyNthDOW')));
  
create table UPSSource (
  SourceID int identity(1,1) not null,
  SourcePackageID int not null,
  SourcePath varchar(550) not null,
  SourceFileName varchar(300) not null,
  constraint UPSSource_pk primary key (SourceID),
  constraint UPSSourcePackage_fk foreign key (SourcePackageID) references UPSPackage(PackageID) on delete cascade,
  constraint UPSSource_Un unique (SourcePackageID,SourcePath,SourceFileName));
  
create table UPSDestination (
  DestinationID int identity(1,1) not null,
  DestinationSourceID int not null,
  DestinationType varchar(20) not null, --Copy, Move, FTP
  DestinationFTPHost varchar(200),
  DestinationFTPUsername varchar(100),
  DestinationFTPPassword varbinary(max),
  DestinationPath varchar(550) not null,
  DestinationFileName varchar(300) not null,
  DestinationAppendDateStampFlag varchar(1) not null default 'N',
  DestinationOverwriteFlag varchar(1) not null default 'Y',
  constraint UPSDestination_pk primary key (DestinationID),
  constraint UPSDestinationSource_fk foreign key (DestinationSourceID) references UPSSource(SourceID) on delete cascade,
  constraint UPSDestination_un unique (DestinationSourceID,DestinationPath,DestinationFileName));
  
create table UPSQueue (
  QueueID int identity(1,1),
  QueuePackageID int not null,
  QueueDateInQueue datetime not null default getdate(),
  QueueProcessFlag varchar(1) not null default 'N',
  QueueScheduledFlag varchar(1) not null default 'N',
  QueueHistoryID int,
  QueueSSISExecutionGUID uniqueIdentifier,
  constraint UPSQueue_pk primary key (QueueID),
  constraint UPSQueuePackage_fk foreign key (QueuePackageID) references UPSPackage(PackageID) on delete cascade,
  constraint UPSQueue_un unique (QueuePackageID));
  
create table UPSHistory (
  HistoryID int identity(1,1) not null,
  HistoryPackageID int not null,
  HistoryStartDate datetime not null default getdate(),
  HistoryEndDate datetime not null default '12/31/4750',
  HistoryStatus varchar(20) not null default 'Processing',
  HistoryScheduledFlag varchar(1) not null default 'N',
  constraint UPSHistory_pk primary key (HistoryID),
  constraint UPSHistoryPackage_fk foreign key (HistoryPackageID) references UPSPackage(PackageID) on delete cascade);
  
create table UPSHistoryDetail (
  HistoryDetailID int identity(1,1) not null,
  HistoryDetailHistoryID int not null,
  HistoryDetailDestinationID int not null,
  HistoryDetailDate datetime not null default getdate(),
  HistoryDetailStatus varchar(20) not null default 'Unknown',
  HistoryDetailErrorMessage varchar(max),
  constraint UPSHistoryDetail_pk primary key (HistoryDetailID),
  constraint UPSHistoryDetailHistory_fk foreign key (HistoryDetailHistoryID) references UPSHistory(HistoryID) on delete cascade,
  constraint UPSHistoryDetailDestination_fk foreign key (HistoryDetailDestinationID) references UPSDestination(DestinationID) on delete no action);
  
go

create view [dbo].[vwUPSHistoryCurrentDay] as
select p.PackageChannel, 
       p.PackageDesc,
       s.SourcePath + s.SourceFileName SourcePathFile,
       d.DestinationPath + d.DestinationFileName DestinationPathFile,
       h.HistoryID, 
       h.HistoryScheduledFlag,
       h.HistoryStartDate,
       h.HistoryEndDate,
       h.HistoryStatus,
       hd.HistoryDetailStatus,
       hd.HistoryDetailErrorMessage  
  from UPSPackage p
  join UPSSource s on s.SourcePackageID = p.PackageID 
  join UPSDestination d on d.DestinationSourceID = s.SourceID 
  join UPSHistory h on h.HistoryPackageID = p.PackageID 
  join UPSHistoryDetail hd on hd.HistoryDetailHistoryID = h.HistoryID 
                          and hd.HistoryDetailDestinationID = d.DestinationID
 where h.HistoryStartDate >= cast(getdate() as date)

go

create view dbo.vwUPSPackageDetails as 
select PackageID,
       PackageChannel,
       PackageDesc,
       PackageOwnerEmail,
       SourcePath,
       SourceFileName,
       DestinationType,
       DestinationPath,
       DestinationFileName,
       DestinationAppendDateStampFlag,
       DestinationOverwriteFlag,
       ScheduleType,
       ScheduleStartTime,
       ScheduleEndTime,
       ScheduleWeekSunFlag,
       ScheduleWeekMonFlag,
       ScheduleWeekTueFlag,
       ScheduleWeekWedFlag,
       ScheduleWeekThuFlag,
       ScheduleWeekFriFlag,
       ScheduleWeekSatFlag,
       PackageRetryAttempts,
       PackageRetryAgainInMins,
       ScheduleEnableFlag
  from dbo.UPSPackage a
  left join dbo.UPSSource s on a.PackageID=s.SourcePackageID
  left join dbo.UPSDestination d on s.SourceID=d.DestinationSourceID
  left join dbo.UPSSchedule sc on a.PackageID=sc.SchedulePackageID

GO

create procedure usp_UPSManageQueue as
begin
  set nocount on
  
  --Local variables
  declare @vTodaysDate datetime,
          @vTodaysDateTime datetime,
          @vTodaysTime int,
          @vTodaysWeek int,
          @vTodaysWeeksInCurrentYear int,
          @vMaxTodayRunStatus varchar(20),
          @vQueueEligible varchar(1),
          @vQueueDateInQueue datetime,
          @vPackageAlreadyInQueue varchar(1),
          @vRetryAttemptExceedEmailSent varchar(1),
          @vActualLastRunDateTime datetime,
          @vActualLastRunStatus varchar(20),
          @vActualLastRunWeek int,
          @vEmailSubject varchar(max),
          @vEmailBody varchar(max);
  
  select @vTodaysDate = cast(convert(varchar(10),getdate(),102) as datetime),
         @vTodaysDateTime = getdate(),
         @vTodaysTime = cast(replace(convert(varchar(5),getdate(),108),':','') as int),
         @vTodaysWeek = datepart(week,getdate()),
         @vTodaysWeeksInCurrentYear = datepart(week,cast('12/31/'+ cast(datepart(year,getdate()) as varchar) as datetime));
  
  --Cursor fetch variables
  declare @vPackageID int,
          @vPackageDesc varchar(150),
          @vPackageOwnerEmail varchar(300),
          @vPackageRetryAttempts int,
          @vPackageRetryAgainInMins int,
          @vScheduleID int,
          @vScheduleType varchar(100),
          @vScheduleInterval int,
          @vScheduleStartTime varchar(5),
          @vScheduleEndTime varchar(5),
          @vScheduleRepeatAfterMins int,
          @vScheduleWeekSunFlag varchar(1),
          @vScheduleWeekMonFlag varchar(1),
          @vScheduleWeekTueFlag varchar(1),
          @vScheduleWeekWedFlag varchar(1),
          @vScheduleWeekThuFlag varchar(1),
          @vScheduleWeekFriFlag varchar(1),
          @vScheduleWeekSatFlag varchar(1),
          @vScheduleMonthDOM int,
          @vScheduleMonthNthOccurance int,
          @vScheduleMonthNthDOW int,
          @vScheduleStartDate datetime,
          @vScheduleEndDate datetime,
          @vMaxTodayRunDate datetime,
          @vHistoryFailCount int;

  declare cur_package cursor local for 
  select p.PackageID, p.PackageDesc, p.PackageOwnerEmail, p.PackageRetryAttempts, p.PackageRetryAgainInMins,
         s.ScheduleID, s.ScheduleType, s.ScheduleInterval, s.ScheduleStartTime, s.ScheduleEndTime, 
         s.ScheduleRepeatAfterMins, s.ScheduleWeekSunFlag, s.ScheduleWeekMonFlag, s.ScheduleWeekTueFlag, 
         s.ScheduleWeekWedFlag, s.ScheduleWeekThuFlag, s.ScheduleWeekFriFlag, s.ScheduleWeekSatFlag, 
         s.ScheduleMonthDOM, s.ScheduleMonthNthOccurance, s.ScheduleMonthNthDOW, s.ScheduleStartDate, s.ScheduleEndDate,
         h.MaxHistoryRunDate, h.HistoryFailCount
    from UPSPackage p 
    join UPSSchedule s on p.PackageID = s.SchedulePackageID
    left outer join (select HistoryPackageID, max(HistoryEndDate) MaxHistoryRunDate, sum(case when HistoryStatus = 'Failed' then 1 else 0 end) HistoryFailCount
                       from UPSHistory h1 
                      where h1.HistoryStartDate >= @vTodaysDate
                        and h1.HistoryStatus in ('Success','Failed')
                      group by HistoryPackageID) h on h.HistoryPackageID = p.PackageID
   where getdate() between s.ScheduleStartDate and s.ScheduleEndDate
     and @vTodaysTime between cast(replace(s.ScheduleStartTime,':','') as int) 
                          and cast(replace(s.ScheduleEndTime,':','') as int);
                                                                            
  open cur_package

  fetch next from cur_package 
   into @vPackageID, @vPackageDesc, @vPackageOwnerEmail, @vPackageRetryAttempts, @vPackageRetryAgainInMins, @vScheduleID,
        @vScheduleType, @vScheduleInterval, @vScheduleStartTime, @vScheduleEndTime, @vScheduleRepeatAfterMins,
        @vScheduleWeekSunFlag, @vScheduleWeekMonFlag, @vScheduleWeekTueFlag, @vScheduleWeekWedFlag,
        @vScheduleWeekThuFlag, @vScheduleWeekFriFlag, @vScheduleWeekSatFlag, @vScheduleMonthDOM, @vScheduleMonthNthOccurance,
        @vScheduleMonthNthDOW, @vScheduleStartDate, @vScheduleEndDate, @vMaxTodayRunDate, @vHistoryFailCount;

  while @@fetch_status = 0
  begin
    --InitVariables
    set @vQueueEligible = 'N';
    set @vQueueDateInQueue = null;
    set @vPackageAlreadyInQueue = 'N';
    set @vRetryAttemptExceedEmailSent = 'N';
    
    --Get the Last Run Status - for today
    if @vMaxTodayRunDate is not null 
    begin
      select @vMaxTodayRunStatus = HistoryStatus
        from UPSHistory 
       where HistoryPackageID = @vPackageID
         and HistoryEndDate = @vMaxTodayRunDate;
    end

    --Get the last Run status and date - for all time
    select @vActualLastRunDateTime = max(HistoryEndDate)
      from UPSHistory
     where HistoryPackageID = @vPackageID
       and HistoryStatus in ('Success','Failed')
       and HistoryStartDate between @vScheduleStartDate and @vScheduleEndDate;
    
    select @vActualLastRunStatus = HistoryStatus
      from UPSHistory
     where HistoryPackageID = @vPackageID
       and HistoryStatus in ('Success','Failed')
       and HistoryEndDate = @vActualLastRunDateTime;
       
    --Determine from schedule when the next run is
    if @vScheduleType = 'Daily' and @vMaxTodayRunDate is null
    begin
      set @vQueueEligible = 'Y';
    end

    /* Future Schedule Enhancements

    if @vScheduleType = 'Weekly' and @vMaxTodayRunDate is null 
                                 and datepart(dw,@vTodaysDate) in (case when @vScheduleWeekSunFlag = 'Y' then 1 else 0 end,
                                                                   case when @vScheduleWeekMonFlag = 'Y' then 2 else 0 end,
                                                                   case when @vScheduleWeekTueFlag = 'Y' then 3 else 0 end,
                                                                   case when @vScheduleWeekWedFlag = 'Y' then 4 else 0 end,
                                                                   case when @vScheduleWeekThuFlag = 'Y' then 5 else 0 end,
                                                                   case when @vScheduleWeekFriFlag = 'Y' then 6 else 0 end,
                                                                   case when @vScheduleWeekSatFlag = 'Y' then 7 else 0 end)
    begin
      if @vScheduleInterval = 1  
        begin
          set @vQueueEligible = 'Y';  
        end
      else
        begin -- This logic handles making sure that the correct week is being run - Needs testing
          if @vTodaysWeek = case when @vActualLastRunWeek + @vScheduleInterval > @vTodaysWeeksInCurrentYear then
                                      @vActualLastRunWeek + @vScheduleInterval - @vTodaysWeeksInCurrentYear
                                 else @vActualLastRunWeek + @vScheduleInterval end
          begin
            set @vQueueEligible = 'N';
          end
        end      
    end

    if @vScheduleType = 'Hourly' and (@vMaxTodayRunDate is null or
                                      dateadd(hour,@vScheduleInterval,@vMaxTodayRunDate) <= @vTodaysDateTime) 
    begin
      set @vQueueEligible = 'Y';  
    end

    if @vScheduleType = 'Monthly' and datepart(day,@vTodaysDate) = @vScheduleMonthDOM and @vMaxTodayRunDate is null
    begin
      if @vScheduleInterval = 1
        begin
          set @vQueueEligible = 'Y';  
        end
      else
        begin
          if (datepart(year,dateadd(month,@vScheduleInterval,@vActualLastRunDateTime)) * 100) + 
              datepart(month,dateadd(month,@vScheduleInterval,@vActualLastRunDateTime)) = 
             (datepart(year,@vTodaysDate) * 100) + datepart(month,@vTodaysDate)
          begin
            set @vQueueEligible = 'Y';  
          end
        end
    end      

    if @vScheduleType = 'MonthlyNthDOW' 
    begin
      set @vQueueEligible = 'N';  
    end
    
    End Future Schedule Enhancements */
        
    --Test to see if any package needs to be retried
    if @vMaxTodayRunStatus = 'Failed' and @vPackageRetryAttempts > 0
    begin
      set @vQueueEligible = 'R';
      set @vQueueDateInQueue = dateadd(minute,@vPackageRetryAgainInMins,@vMaxTodayRunDate);
    end

    --Test to see if the Job has exceeded the number of retries
    if @vHistoryFailCount >= @vPackageRetryAttempts and @vPackageRetryAttempts > 0
    begin
      set @vQueueEligible = 'N';
      select @vRetryAttemptExceedEmailSent = case when count(*) > 0 then 'Y' else 'N' end 
        from UPSHistory
       where HistoryPackageID = @vPackageID
         and HistoryStartDate >= @vTodaysDate
         and HistoryStatus = 'RetryFailEmail';

      if @vRetryAttemptExceedEmailSent = 'N'
      begin
        insert into UPSHistory (HistoryPackageID,HistoryStartDate,HistoryEndDate,HistoryStatus,HistoryScheduledFlag)
                        values (@vPackageID,@vTodaysDateTime,@vTodaysDateTime,'RetryFailEmail','N');
        select @vEmailSubject = 'UPS: Failed Retry Attempts Exceeded - ' + @vPackageDesc,
               @vEmailBody = 'This Package will not retry again today and will resume it''s usual schedule starting tomorrow.'
        exec msdb.dbo.sp_send_dbmail @recipients = @vPackageOwnerEmail, @subject = @vEmailSubject, @body = @vEmailBody
      end

    end
     
    --Check to see if package is already in Queue
    select @vPackageAlreadyInQueue = case when count(*) > 0 then 'Y' else 'N' end
      from UPSQueue q
     where q.QueuePackageID = @vPackageID;
    
    --Add to Queue
    if @vQueueEligible in ('Y','R') and @vPackageAlreadyInQueue = 'N'
    begin
      select @vEmailSubject = cast(@vPackageID as varchar) + ' - ' + @vPackageDesc + case when @vQueueEligible = 'R' then ' (Retry # ' + cast(@vHistoryFailCount + 1 as varchar) + ' of ' + cast(@vPackageRetryAttempts as varchar) + ')' end;
      print @vEmailSubject;
      insert into UPSQueue (QueuePackageID,QueueDateInQueue,QueueScheduledFlag)
                    values (@vPackageID,
                            case when @vQueueEligible = 'R' then isnull(@vQueueDateInQueue,@vTodaysDateTime) else @vTodaysDateTime end,
                            case when @vQueueEligible = 'R' then 'N' else 'Y' end);
                            
      if @vQueueEligible = 'R'
      begin
        select @vEmailSubject = 'UPS: Retry Attempt added to Queue for processing - ' + @vPackageDesc,
               @vEmailBody = 'This Package will retry again on ' + convert(varchar(20),isnull(@vQueueDateInQueue,@vTodaysDateTime),120) + '. (Retry # ' + cast(@vHistoryFailCount + 1 as varchar) + ' of ' + cast(@vPackageRetryAttempts as varchar) + ')' 
        --exec msdb.dbo.sp_send_dbmail @recipients = @vPackageOwnerEmail, @subject = @vEmailSubject, @body = @vEmailBody;
      end                            
    end
     
    --Move to next package
    fetch next from cur_package 
     into @vPackageID, @vPackageDesc, @vPackageOwnerEmail, @vPackageRetryAttempts, @vPackageRetryAgainInMins, @vScheduleID,
          @vScheduleType, @vScheduleInterval, @vScheduleStartTime, @vScheduleEndTime, @vScheduleRepeatAfterMins,
          @vScheduleWeekSunFlag, @vScheduleWeekMonFlag, @vScheduleWeekTueFlag, @vScheduleWeekWedFlag,
          @vScheduleWeekThuFlag, @vScheduleWeekFriFlag, @vScheduleWeekSatFlag, @vScheduleMonthDOM, @vScheduleMonthNthOccurance,
          @vScheduleMonthNthDOW, @vScheduleStartDate, @vScheduleEndDate, @vMaxTodayRunDate, @vHistoryFailCount;

  end 
  close cur_package
  deallocate cur_package

end;

go

create function ufnc_UPSEncrypt (@pEncryptMe varchar(max)) returns varbinary(max) as
begin
  return encryptbypassphrase('185B0E8A-90E3-4B9B-959C-0F3EEA5E9F4A',@pEncryptMe)
end;

go

create function ufnc_UPSDecrypt (@pDecryptMe varbinary(max)) returns varchar(max) as
begin
  return decryptbypassphrase('185B0E8A-90E3-4B9B-959C-0F3EEA5E9F4A',@pDecryptMe)
end;

go

create procedure usp_UPSWebGetPackageStatus as
begin
  set nocount on;
  
  select PackageID,
         PackageDesc,
         case s.ScheduleType when 'Daily' then 'Every ' + cast(s.ScheduleInterval as varchar) + ' day(s), between ' + s.ScheduleStartTime + ' and ' + s.ScheduleEndTime 
                             else 'Unsupported Schedule Type' end ScheduleDesc,
         h.HistoryEndDate LastRunDate,
         h.HistoryStatus LastRunStatus
    from UPSPackage p
    join UPSSchedule s on s.SchedulePackageID = p.PackageID
                      and getdate() between s.ScheduleStartDate and s.ScheduleEndDate
    left outer join (select *
                       from UPSHistory h1
                      where h1.HistoryStartDate = (select max(HistoryStartDate)
                                                    from UPSHistory h2
                                                   where h1.HistoryPackageID = h2.HistoryPackageID)) h on h.HistoryPackageID = p.PackageID
    left outer join (select historyPackageID,                                                    
   order by p.PackageDesc;
end;

go

create procedure usp_UPSAddPackageToQueue (@pPackageID int)  --Required
as
begin
  set nocount on 
  declare @vPackageValid varchar(1) = 'N',
          @vPackageAlreadyInQueue varchar(1) = 'N'
  
  --Check to make sure the package is valid
  select @vPackageValid = case when count(*) = 0 then 'N' else 'Y' end
    from UPSPackage p
    join UPSSource s on p.PackageID = s.SourcePackageID 
    join UPSDestination d on s.SourceID = d.DestinationSourceID 
   where p.PackageID = @pPackageID 
  
  if @vPackageValid = 'N'
  begin
    raiserror('The PackageID is not valid.',11,1)
    return
  end 
  
  --Check to see if the Package is already in Queue
  select @vPackageAlreadyInQueue = case when count(*) = 0 then 'N' else 'Y' end 
    from UPSQueue
   where QueuePackageID = @pPackageID
  
  if @vPackageAlreadyInQueue = 'Y'
  begin
    raiserror('The PackageID is already in Queue.',11,2)
    return
  end 
   
  --Add to the Queue
  insert into UPSQueue (QueuePackageID,QueueDateInQueue,QueueScheduledFlag)
   values (@pPackageID, getdate(),'N')
   
end

go

create procedure usp_UPSSrcDestChange (@pSourceID int = null, --Required
                                       @pNewSourceFileName varchar(300) = null, --Required
                                       @pNewDestinationFileName varchar(300) = null, --Non Distructive
                                       @pNewDestinationAppendDateStamp varchar(1) = null, --Non Distructive
                                       @pNewDestinationOverwriteFlag varchar(1) = null, --Non Distructive
                                       @pAddPackageToQueue varchar(1) = 'N') 
                                               
as
begin
  set nocount on;
  declare @vPackageID int,
          @vPackageAlreadyInQueue varchar(1) = 'Y'
  
  --Check to see that the paramaters needed are valid
  if @pSourceID is not null
  begin
    select @vPackageID = SourcePackageID 
      from UPSSource 
     where SourceID = @pSourceID
  end
  
  if @vPackageID is null and @pSourceID is not null
  begin
    raiserror('The SourceID is invalid.',11,1)
    return
  end

  if @pSourceID is null    
  begin
    raiserror('SourceID is a required paramater.',11,2)
    return
  end
 
  if isnull(@pNewSourceFileName,'') = ''
  begin
    raiserror('NewSourceFileName is a required paramater.',11,3)
    return
  end

  if @pNewDestinationAppendDateStamp is not null and @pNewDestinationAppendDateStamp not in ('Y','N')
  begin
    raiserror('NewDestinationAppendDateStamp must be either null, Y or N.',11,4)
    return
  end

  if @pNewDestinationOverwriteFlag is not null and @pNewDestinationOverwriteFlag not in ('Y','N')
  begin
    raiserror('NewDestinationOverwriteFlag must be either null, Y or N.',11,5)
    return
  end

  if @pAddPackageToQueue is null or isnull(@pAddPackageToQueue,'A') not in ('Y','N')
  begin
    raiserror('AddPackageToQueue must be either Y or N.',11,6)
    return
  end

  --Check to make sure Package is not in Queue
  select @vPackageAlreadyInQueue = case when count(*) = 0 then 'N' else 'Y' end 
    from UPSQueue
   where QueuePackageID = @vPackageID

  if @vPackageAlreadyInQueue = 'Y'   
  begin
    raiserror('The PackageID is already in Queue. Changes cannot be made until the current package is complete.',11,7)
    return
  end

  --Update UPSSource records
  update UPSSource 
     set SourceFileName = @pNewSourceFileName
   where SourceID = @pSourceID 
     
  --Update UPSDestination
  update UPSDestination
     set DestinationFileName = isnull(@pNewDestinationFileName,DestinationFileName),
         DestinationAppendDateStampFlag = isnull(@pNewDestinationAppendDateStamp,DestinationAppendDateStampFlag), 
         DestinationOverwriteFlag = isnull(@pNewDestinationOverwriteFlag,DestinationOverwriteFlag)
   where DestinationSourceID = @pSourceID
  
  if @pAddPackageToQueue = 'Y'
    exec usp_UPSAddPackageToQueue @pPackageID = @vPackageID
    
end;

go
