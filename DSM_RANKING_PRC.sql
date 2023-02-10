DROP PROCEDURE IF EXISTS hocsql.DSM_RANKING_PRC;

DELIMITER $$
$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `hocsql`.`DSM_RANKING_PRC`()
begin    
	/*
	 * -- step 1 : di tinh lai so lieu binh quan dua tren doanh so tung thang cua cac ong DSM ( LTN , PSDN , APPROVED_RATE)
	 * -- step 2 : 
		công thức : 
	    -- 1 tinh npl truoc write off luy ke đứng ở thời điểm tháng 5 
		-- X : sum (số tiền wo các tài khoản bị wo tháng 1 , số tiền wo các tài khoản bị wo tháng 2 .. tháng 5 ) : số tiền wo lũy kế trong năm 
		-- Z dư nợ cuối kỳ ở thời điểm tháng 5 sau wo ( chỉ sum OUTSTANDING_PRINCIPAL)
		-- Y dư nợ cuối kỳ của nhóm 3,4,5 ở thời điểm tháng 5 sau wo (chỉ sum OUTSTANDING_PRINCIPAL)
		-- (X + Y )* 100 / ( Z+Y) -> phân bổ theo branch : NPL tháng 5 
		-- AVG NPL =  (NPL write off luy ke tháng 1 + NPL write off luy ke tháng 2 + .. NPL write off luy ke tháng 5 ) / 5
		
		-- step 2a : tao bảng list_month_key để lưu các tháng từ đầu năm đến tháng báo cáo 
		-- step 2b : write off lũy kế theo từng tháng 
		-- step 2c : tính dư nợ cuối kỳ theo branch city , theo tháng -> đổ dữ liệu avg npl theo branch_city , month_key = tháng báo cáo
   */
	declare vMonthkey BIGINT;
	declare vBegin_Monthkey bigint;
	-- step 1 : di tinh lai so lieu binh quan dua tren doanh so tung thang cua cac ong DSM ( LTN , PSDN , APPROVED_RATE)
	-- gan gia tri vao bien month_key 
	set vMonthkey := (select max(month_key) from doanh_so_dsm_stg dsds) ; 
    set vBegin_Monthkey := (select concat(substring('202205',1,4), '01')) ; 
    select vBegin_Monthkey;
     /*
     * cap nhat ten branch city khong trung voi danh sach dsm import vao
     */
    
    update doanh_so_dsm_stg 
    set Province = 'Bà Rịa - Vũng Tàu'
    where Province = 'Vũng Tàu' ; commit;
   
    update doanh_so_dsm_stg 
    set Province = 'Thừa Thiên Huế'
    where Province = 'Huế' ; commit;
    
    delete from doanh_so_dsm_avg where month_key = vMonthkey; commit;
	-- do du lieu -- avg 
    insert into doanh_so_dsm_avg
	select month_key , Province , Staff_Code_DSM,
	-- case when 12 thang -- 
	-- LTN_AVG -- 
	(ifnull(LTN_01,0) + ifnull(LTN_02,0) + ifnull(LTN_03,0) + ifnull(LTN_04,0) + ifnull(LTN_05,0))
	/ 
	( case 
			when LTN_01 is null then 0 
			else 1
	  end  +
	  case  
			when LTN_02 is null then 0 
			else 1
	  end  +
	  case  
			when LTN_03 is null then 0 
			else 1
	  end  +
	  case 
			when LTN_04 is null then 0 
			else 1
	  end  +
	  case 
			when LTN_05 is null then 0 
			else 1
	  end  
	) as LTN_AVG,
	-- PSDN_AVG -- 
	(ifnull(PSDN_01,0) + ifnull(PSDN_02,0) + ifnull(PSDN_03,0) + ifnull(PSDN_04,0) + ifnull(PSDN_05,0))
	/ 
	( case 
			when PSDN_01 is null then 0 
			else 1
	  end  +
	  case 
			when PSDN_02 is null then 0 
			else 1
	  end  +
	  case 
			when PSDN_03 is null then 0 
			else 1
	  end  +
	  case 
			when PSDN_04 is null then 0 
			else 1
	  end  +
	  case 
			when PSDN_05 is null then 0 
			else 1
	  end  
	) as PSDN_AVG,
	-- APPROVAL_RATE_AVG : sum(APPROVED) / sum (APP_IN)-- 
	-- rang 0 
	case
		when (ifnull(APPIN_01,0) + ifnull(APPIN_02,0) + ifnull(APPIN_03,0) + ifnull(APPIN_04,0) + ifnull(APPIN_05,0)) = 0 then 0
		else 
		100*(ifnull(APPROVED_01,0) + ifnull(APPROVED_02,0) + ifnull(APPROVED_03,0) + ifnull(APPROVED_04,0) + ifnull(APPROVED_05,0))
		/ 
		(ifnull(APPIN_01,0) + ifnull(APPIN_02,0) + ifnull(APPIN_03,0) + ifnull(APPIN_04,0) + ifnull(APPIN_05,0)) 
	end 
	as APPROVAL_RATE_AVG
	from doanh_so_dsm_stg dsds ;
    commit;
   

    
   	-- step 2 : 
    -- 1 tinh npl truoc write off luy ke đứng ở thời điểm tháng 5 
	-- X : sum (số tiền wo các tài khoản bị wo tháng 1 , số tiền wo các tài khoản bị wo tháng 2 .. tháng 5 ) : số tiền wo lũy kế trong năm : chỉ đơn vị mạng lưới
	-- Z dư nợ cuối kỳ ở thời điểm tháng 5 sau wo ( chỉ sum OUTSTANDING_PRINCIPAL) : chỉ lấy đơn vị mạng lưới 
	-- Y dư nợ cuối kỳ của nhóm 3,4,5 ở thời điểm tháng 5 sau wo (chỉ sum OUTSTANDING_PRINCIPAL)
	-- (X + Y )* 100 / ( Z+Y) -> phân bổ theo branch : NPL tháng 5 
	-- AVG NPL =  (NPL write off luy ke tháng 1 + NPL write off luy ke tháng 2 + .. NPL write off luy ke tháng 5 ) / 5
   
   -- danh sach thang tu dau nam den thang bao can bao cao 
   truncate table LIST_MONTH_KEY  ;
   insert into LIST_MONTH_KEY
	WITH RECURSIVE cte_count (n) 
	AS (
	      select concat(substr(vMonthkey,1,4) , "01")
	      UNION ALL
	      SELECT n + 1 
	      FROM cte_count 
	      WHERE n < vMonthkey 
	    )
	SELECT n 
	FROM cte_count; commit;
   
   -- danh sach so tien write off luy ke theo tung thang bao cao : write off Year to date (YTD)
    truncate table  WO_LUY_MONTH_BY_BRANCH_CITY;
   insert into WO_LUY_MONTH_BY_BRANCH_CITY 
	select y.month_key  , x.BRANCH_CITY , sum(SO_TIEN_WO_HACH_TOAN_MOI_THANG) as WO_LUY_MONTH
	from 
	(
		select BRANCH_CITY  , KPI_MONTH , sum(WRITE_OFF_BALANCE_PRINCIPAL) as SO_TIEN_WO_HACH_TOAN_MOI_THANG 
		from kpi_raw_data
		where WRITE_OFF_MONTH is not null 
		and KPI_MONTH = WRITE_OFF_MONTH 
		-- chi lay kenh don vi mang luoi 
		and pos_type_key not in (4,6)
		group by KPI_MONTH , BRANCH_CITY 
	) X 
	-- danh sach thang 
	join LIST_MONTH_KEY Y on (X.KPI_MONTH <= Y.month_key)
	group by Y.month_key , x.BRANCH_CITY
	order by x.BRANCH_CITY , Y.month_key ;

   -- duno cuoi ky theo branch_city theo thang month_key -> npl thoi diem branch_city , month_key -> group by branch_city : tính ra npl avg

   truncate table DNCK_BRANCH_CITY_MONTHKEY ;
   insert into DNCK_BRANCH_CITY_MONTHKEY
   select  KPI_MONTH , branch_city , sum(OUTSTANDING_PRINCIPAL) as DNCK_BRANCH_CITY ,
	sum(case
	when MAX_BUCKET in (3,4,5) then OUTSTANDING_PRINCIPAL
	else 0
	end) as DNCK_NHOM_345,
	sum(
			case
				when ifnull(MAX_BUCKET,1) = 1 then OUTSTANDING_PRINCIPAL
				else 0 
			end
		) as DNCK_DB_GROUP_1,
		sum(
			case
				when ifnull(MAX_BUCKET,1) = 2 then OUTSTANDING_PRINCIPAL
				else 0 
			end
		) as DNCK_DB_GROUP_2,
		sum(
			case
				when ifnull(MAX_BUCKET,1) <> 1 then OUTSTANDING_PRINCIPAL
				else 0 
			end
		) as DNCK_DB_GROUP_2_5
	from kpi_raw_data X 
	where 
	-- chi lay kenh don vi mang luoi 
	pos_type_key not in (4,6)
	and kpi_month between vBegin_Monthkey and vMonthkey 
	group by branch_city , KPI_MONTH ;

-- DNCK AVG SAU WO DEN THANG BAO CAO
    truncate table AVG_DNCK_BRANCH_CITY ;
   insert into AVG_DNCK_BRANCH_CITY
	 select vMonthkey as MONTH_KEY , branch_city , 
	 sum(DNCK_BRANCH_CITY) / count(1) as AVG_DNCK_BRANCH_CITY,
	 sum(DNCK_NHOM_345) / count(1) as AVG_DNCK_NHOM_345,
	 sum(DNCK_DB_GROUP_1) / count(1) as AVG_DNCK_DB_GROUP_1,
	 sum(DNCK_DB_GROUP_2) / count(1) as AVG_DNCK_DB_GROUP_2,
	 sum(DNCK_DB_GROUP_2_5) / count(1) as AVG_DNCK_DB_GROUP_2_5
	 from DNCK_BRANCH_CITY_MONTHKEY
	 group by branch_city ; commit;

-- DNCK AVG TRUOC WO DEN THANG BAO CAO
truncate table AVG_DNCK_BRANCH_CITY_TRUOC_WO ;
  insert into AVG_DNCK_BRANCH_CITY_TRUOC_WO
	 select vMonthkey as MONTH_KEY , x.branch_city , 
	 sum(DNCK_BRANCH_CITY + ifnull(WO_LUY_MONTH,0)) / count(1) as AVG_DNCK_BRANCH_CITY_TRUOC_WO,
	 sum(DNCK_NHOM_345 + ifnull(WO_LUY_MONTH,0)) / count(1) as AVG_DNCK_NHOM_345_TRUOC_WO,
	 sum(DNCK_DB_GROUP_1 + ifnull(WO_LUY_MONTH,0)) / count(1) as AVG_DNCK_DB_GROUP_1_TRUOC_WO,
	 sum(DNCK_DB_GROUP_2 + ifnull(WO_LUY_MONTH,0)) / count(1) as AVG_DNCK_DB_GROUP_2_TRUOC_WO,
	 sum(DNCK_DB_GROUP_2_5 + ifnull(WO_LUY_MONTH,0)) / count(1) as AVG_DNCK_DB_GROUP_2_5_TRUOC_WO
	 from DNCK_BRANCH_CITY_MONTHKEY x
	 left join wo_luy_month_by_branch_city y on x.KPI_MONTH = y.month_key and x.branch_city =y.BRANCH_CITY 
	 group by x.branch_city ; commit;
	
	-- PSDN theo branch_city --
truncate table PSDN_BRANCH_CITY ;
	insert into PSDN_BRANCH_CITY
	select vMonthkey as month_key , BRANCH_CITY  , sum(PSDN) as PSDN_BRANCH_CITY 
	from kpi_raw_data 
	where KPI_MONTH between vBegin_Monthkey and vMonthkey
	group by BRANCH_CITY ;
    
/*
	1.CIR : 	Tổng C.Phí H.động / Tổng T.Nhập H.động : CIR
	2.Margin	Lợi nhuận trước thuế/Tổng doanh thu : ( tổng thu nhập 
	3. Hệ số/Vốn	Lợi nhuận trước thuế/Giá vốn thuần ( khả năng sử dụng vốn 100 tỷ -> sử dụng hết 100 tỷ -> 70-80 tỷ )
	4.Năng suất/Headcount	Lợi nhuận trước thuế/Tổng nhân sự ( vùng , tỉnh )
*/
	-- do du lieu vao bang target -- 
    delete from chi_so_theo_dsm_report where month_key = vMonthkey; commit;
	insert into chi_so_theo_dsm_report
	select x.* ,
	y.NPL_TRUOC_WO_LUY_KE,
	null as CIR,
	null as MARGIN,
	null as HS_TREN_VON,
	null as NS_TREN_HEADCOUNT
	from 
	doanh_so_dsm_avg x
	-- lay chi so npl truoc wo theo tinh
	left join 
	(
		select x.month_key , x.branch_city ,
		(x.DNCK_NHOM_345 + ifnull(y.WO_LUY_MONTH,0) ) * 100 / (x.DNCK_BRANCH_CITY + ifnull(y.WO_LUY_MONTH,0) ) as NPL_TRUOC_WO_LUY_KE 
		from DNCK_BRANCH_CITY_MONTHKEY x 
		left join WO_LUY_MONTH_BY_BRANCH_CITY y on x.month_key = y.month_key and x.branch_city  = y.BRANCH_CITY 
		-- them dieu kien lay thang hien tai
		where x.month_key = vMonthkey 
	) y on x.month_key = y.month_key and x.Province = y.branch_city
	where x.month_key = vMonthkey; commit;


-- 4 chi so ve tai chinh :
-- step 1 : tinh gl theo tung tinh 
-- step 2 : tinh dnck avg sau wo , truoc wo * so tiền không phân bổ được để phân bổ theo từng đầu mục GL   
-- tinh CIR -- 
truncate table TONG_CP_HOAT_DONG_BRANCH_CITY_PB;commit;
insert into TONG_CP_HOAT_DONG_BRANCH_CITY_PB 
select MA_BRANCH_CITY ,BRANCH_CITY , 
ifnull(CP_THUE_PHI_BRANCH_CITY_DPB,0) as CP_THUE_PHI_BRANCH_CITY_DPB,
ifnull(CP_NHAN_VIEN_BRANCH_CITY_DPB,0) as CP_NHAN_VIEN_BRANCH_CITY_DPB,
ifnull(CP_QUAN_LY_BRANCH_CITY_DPB,0) as CP_QUAN_LY_BRANCH_CITY_DPB,
ifnull(CP_TAI_SAN_BRANCH_CITY_DPB,0) as CP_TAI_SAN_BRANCH_CITY_DPB,
ifnull(CP_THUE_PHI_BRANCH_CITY_DPB,0) +
ifnull(CP_NHAN_VIEN_BRANCH_CITY_DPB,0) +
ifnull(CP_QUAN_LY_BRANCH_CITY_DPB,0) + 
ifnull(CP_TAI_SAN_BRANCH_CITY_DPB,0) as TONG_CP_HOAT_DONG_BRANCH_CITY
from 
(
	select x.DD as MA_BRANCH_CITY, x.`Tỉnh/TP` as BRANCH_CITY ,
	ifnull(y.CP_THUE_PHI_BRANCH_CITY,0) + (PHAN_BO_CP_THUE_PHI * SLNV_BRANCH_CITY / TOTAL_NV) as  CP_THUE_PHI_BRANCH_CITY_DPB,
	ifnull(y.CP_NHAN_VIEN_BRANCH_CITY,0) + (PHAN_BO_CP_NHAN_VIEN * SLNV_BRANCH_CITY / TOTAL_NV) as  CP_NHAN_VIEN_BRANCH_CITY_DPB,
	ifnull(y.CP_QUAN_LY_BRANCH_CITY,0) + (PHAN_BO_CP_QUAN_LY * SLNV_BRANCH_CITY / TOTAL_NV) as  CP_QUAN_LY_BRANCH_CITY_DPB,
	ifnull(y.CP_TAI_SAN_BRANCH_CITY,0) + (PHAN_BO_CP_TAI_SAN * SLNV_BRANCH_CITY / TOTAL_NV) as  CP_TAI_SAN_BRANCH_CITY_DPB
	from province_dim x
	-- Tổng chi phí hoạt động dau GL  
	left join 
	(
		select vMonthkey as month_key , substring(ANALYSIS_CODE,10,2) as MA_BRANCH_CITY,
		/*
		 *  CP thuế, phí	KT	GL	831000000001,831000000002,832000000101,832000000001,831000000102
			CP nhân viên	KT	GL	85x
			CP quản lý	KT	GL	86x
			CP tài sản	KT	GL	87x
		 */
		sum(
		case 
			when ACCOUNT_CODE in (831000000001,831000000002,832000000101,832000000001,831000000102) then AMOUNT 
			else 0
		end ) as CP_THUE_PHI_BRANCH_CITY ,
		sum(
		case 
			when ACCOUNT_CODE like '85%' then AMOUNT 
			else 0
		end ) as CP_NHAN_VIEN_BRANCH_CITY ,
		sum(
		case 
			when ACCOUNT_CODE like '86%' then AMOUNT 
			else 0
		end ) as CP_QUAN_LY_BRANCH_CITY ,
		sum(
		case 
			when ACCOUNT_CODE like '87%' then AMOUNT 
			else 0
		end ) as CP_TAI_SAN_BRANCH_CITY 
		from txn_raw_data_gl trdg 
		where ANALYSIS_CODE like 'DVS%'
		and substring(ANALYSIS_CODE,10,2) <> '00'
	-- 	and substring(ANALYSIS_CODE,10,2) = '01'
		and date_format(TRANSACTION_DATE,'%Y%m') between vBegin_Monthkey and vMonthkey 
		group by substring(ANALYSIS_CODE,10,2)
	) y on x.dd = y.MA_BRANCH_CITY
	left join 
	(
		select Province as BRANCH_CITY , count(1) as SLNV_BRANCH_CITY , 
			(
				select count(1) as SL_DSM 
				from doanh_so_dsm_avg
			) as TOTAL_NV
			from doanh_so_dsm_avg
			group by Province
	) z on x.`Tỉnh/TP` = z.BRANCH_CITY
	-- CHI PHI CAN PHAN BO 
	left join 
	(
		select vMonthkey as month_key , substring(ANALYSIS_CODE,10,2) as MA_BRANCH_CITY,
		/*
		 *  CP thuế, phí	KT	GL	831000000001,831000000002,832000000101,832000000001,831000000102
			CP nhân viên	KT	GL	85x
			CP quản lý	KT	GL	86x
			CP tài sản	KT	GL	87x
		 */
		sum(
		case 
			when ACCOUNT_CODE in (831000000001,831000000002,832000000101,832000000001,831000000102) then AMOUNT 
			else 0
		end ) as PHAN_BO_CP_THUE_PHI ,
		sum(
		case 
			when ACCOUNT_CODE like '85%' then AMOUNT 
			else 0
		end ) as PHAN_BO_CP_NHAN_VIEN ,
		sum(
		case 
			when ACCOUNT_CODE like '86%' then AMOUNT 
			else 0
		end ) as PHAN_BO_CP_QUAN_LY ,
		sum(
		case 
			when ACCOUNT_CODE like '87%' then AMOUNT 
			else 0
		end ) as PHAN_BO_CP_TAI_SAN
		from txn_raw_data_gl trdg 
		where ANALYSIS_CODE like 'DVS%'
		and substring(ANALYSIS_CODE,10,2) = '00'
		and date_format(TRANSACTION_DATE,'%Y%m') between vBegin_Monthkey and vMonthkey
		group by substring(ANALYSIS_CODE,10,2)
	) pb on (1=1)
	-- loai ra thang chung 
	where dd <> '00'
) A ; commit;


	-- lay tong thu nhap tu hoat dong the -- 
    truncate table TONG_THU_NHAP_HD_THE ;
   insert into TONG_THU_NHAP_HD_THE
select vMonthkey as month_key , substring(ANALYSIS_CODE,10,2) as MA_BRANCH_CITY,
		/*
		 *  Lãi trong hạn	KT	GL	702000030002, 702000030001,702000030102
		 * Lãi quá hạn	KT	GL	702000030012, 702000030112
			
			Phí bảo hiểm	KT	GL	716000000001
			Phí tăng hạn mức	KT	GL	719000030002
			Phí thanh toán chậm, thu từ ngoại bảng	KT	GL	719000030003,719000030103,790000030003,790000030103,790000030004,790000030104
		 */
-- lai trong han 
		sum(
		case 
			when ACCOUNT_CODE in (702000030002, 702000030001,702000030102) then AMOUNT 
			else 0
		end ) as LAI_TRONG_HAN_BRANCH_CITY ,
		-- lai qua han 
		sum(
		case 
			when ACCOUNT_CODE in (702000030012, 702000030112)  then AMOUNT 
			else 0
		end ) as LAI_QUA_HAN_BRANCH_CITY ,
		-- phi bao hiem 
		sum(
		case 
			when ACCOUNT_CODE in (716000000001) then AMOUNT 
			else 0
		end ) as PHI_BAO_HIEM_BRANCH_CITY ,
		-- phi tang han muc 
		sum(
		case 
			when ACCOUNT_CODE in (719000030002) then AMOUNT 
			else 0
		end ) as PHI_TANG_HM_BRANCH_CITY ,
		-- phi thanh toan cham thu tu ngoai bang -- 
		sum(
		case 
			when ACCOUNT_CODE in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104) then AMOUNT 
			else 0
		end ) as PHI_TRA_CHAM_BRANCH_CITY 
		from txn_raw_data_gl trdg 
		where ANALYSIS_CODE like 'DVS%'
		and date_format(TRANSACTION_DATE,'%Y%m') between vBegin_Monthkey and vMonthkey
		group by substring(ANALYSIS_CODE,10,2) ;
	 commit;
	
	-- phan bo thu nhap hoat dong the theo dnck binh quan -- 
	truncate table TONG_THU_NHAP_HD_THE_PHAN_BO ;
	insert into TONG_THU_NHAP_HD_THE_PHAN_BO
	select 
	x.branch_city , 
	x.LAI_TRONG_HAN_BRANCH_CITY + ifnull((x.LAI_TRONG_HAN_BRANCH_CITY_HO * y.AVG_DNCK_DB_GROUP_1 / z.AVG_DNCK_DB_GROUP_1_HO),0)
	as LAI_TRONG_HAN_BRANCH_CITY_PB,
	x.LAI_QUA_HAN_BRANCH_CITY + ifnull((x.LAI_QUA_HAN_BRANCH_CITY_HO * y.AVG_DNCK_DB_GROUP_2 / z.AVG_DNCK_DB_GROUP_2_HO),0)
	as LAI_QUA_HAN_BRANCH_CITY_PB,
	x.PHI_BAO_HIEM_BRANCH_CITY + ifnull((x.PHI_BAO_HIEM_BRANCH_CITY_HO * m.PSDN_BRANCH_CITY / n.PSDN_BRANCH_CITY_HO),0)
	as PHI_BAO_HIEM_BRANCH_CITY_PB,
	x.PHI_TANG_HM_BRANCH_CITY + ifnull((x.PHI_TANG_HM_BRANCH_CITY_HO * y.AVG_DNCK_DB_GROUP_1 / z.AVG_DNCK_DB_GROUP_1_HO),0)
	as PHI_TANG_HM_BRANCH_CITY_PB,
	x.PHI_TRA_CHAM_BRANCH_CITY + ifnull((x.PHI_TRA_CHAM_BRANCH_CITY_HO * y.AVG_DNCK_DB_GROUP_2_5 / z.AVG_DNCK_DB_GROUP_2_5_HO),0)
	as PHI_TRA_CHAM_BRANCH_CITY_PB
	from 
	(
		select 
		z.`Tỉnh/TP` as branch_city,
		x.LAI_TRONG_HAN_BRANCH_CITY,
		x.LAI_QUA_HAN_BRANCH_CITY,
		x.PHI_BAO_HIEM_BRANCH_CITY,
		x.PHI_TANG_HM_BRANCH_CITY,
		x.PHI_TRA_CHAM_BRANCH_CITY ,
		y.LAI_TRONG_HAN_BRANCH_CITY as LAI_TRONG_HAN_BRANCH_CITY_HO,
		y.LAI_QUA_HAN_BRANCH_CITY as LAI_QUA_HAN_BRANCH_CITY_HO,
		y.PHI_BAO_HIEM_BRANCH_CITY as PHI_BAO_HIEM_BRANCH_CITY_HO,
		y.PHI_TANG_HM_BRANCH_CITY as PHI_TANG_HM_BRANCH_CITY_HO,
		y.PHI_TRA_CHAM_BRANCH_CITY as PHI_TRA_CHAM_BRANCH_CITY_HO
		from TONG_THU_NHAP_HD_THE x
		join TONG_THU_NHAP_HD_THE y on y.MA_BRANCH_CITY  = '00'
		left join province_dim z on x.MA_BRANCH_CITY = z.DD 
	) x 
	left join avg_dnck_branch_city y on x.branch_city = y.branch_city 
	left join 
	(
		select 
		sum(AVG_DNCK_DB_GROUP_1) as AVG_DNCK_DB_GROUP_1_HO,
		sum(AVG_DNCK_DB_GROUP_2) as AVG_DNCK_DB_GROUP_2_HO,
		sum(AVG_DNCK_DB_GROUP_2_5) as AVG_DNCK_DB_GROUP_2_5_HO
		from avg_dnck_branch_city
	) z on (1=1)
	left join PSDN_BRANCH_CITY m on x.branch_city = m.branch_city 
	left join 
	(
		select sum(PSDN_BRANCH_CITY) as PSDN_BRANCH_CITY_HO
		from PSDN_BRANCH_CITY
	) n on (1=1);


	-- tinh CP_THUAN_HOAT_DONG_KHAC chua phan bổ -- 
	truncate table CP_THUAN_HOAT_DONG_KHAC ;
insert into CP_THUAN_HOAT_DONG_KHAC
select vMonthkey as month_key , substring(ANALYSIS_CODE,10,2) as MA_BRANCH_CITY,
		/*
		 *  
		 * CP hoa hồng	KT	GL	816000000001,816000000002,816000000003
CP thuần KD khác	KT	GL	809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001
DT kinh doanh	KT	GL	702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101
4
		 */
-- CP HOA HONG
		sum(
		case 
			when ACCOUNT_CODE in (816000000001,816000000002,816000000003) then AMOUNT 
			else 0
		end ) as CP_HOA_HONG_BRANCH_CITY ,
		-- CP THUAN KD KHAC
		sum(
		case 
			when ACCOUNT_CODE in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001
			,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001
			,790000000003,790000050101,790000000101
			,790037000001,849000000001,899000000003
			,899000000002,811000000101,819000060001)  then AMOUNT 
			else 0
		end ) as CP_THUAN_KD_KHAC_BRANCH_CITY ,
		-- DT KINH DOANH
		sum(
		case 
			when ACCOUNT_CODE in (702000010001,702000010002,704000000001,
			705000000001,709000000001,714000000002,714000000003,
			714037000001,714000000004,714014000001,715000000001,
			715037000001,719000000001,709000000101,719000000101) then AMOUNT 
			else 0
		end ) as CP_DT_KINH_DOANH_BRANCH_CITY 
		from txn_raw_data_gl trdg 
		where ANALYSIS_CODE like 'DVS%'
-- 		and substring(ANALYSIS_CODE,10,2) <> '00'
		and date_format(TRANSACTION_DATE,'%Y%m') between vBegin_Monthkey and vMonthkey
		group by substring(ANALYSIS_CODE,10,2);
	commit;

	-- phan bo chi phi thuan hoat dong khac -- 
	truncate table CP_THUAN_HOAT_DONG_KHAC_PHAN_BO ;
	insert into CP_THUAN_HOAT_DONG_KHAC_PHAN_BO 
	select x.BRANCH_CITY ,
	x.CP_HOA_HONG_BRANCH_CITY + ifnull((x.CP_HOA_HONG_BRANCH_CITY_HO * y.AVG_DNCK_BRANCH_CITY / z.AVG_DNCK_BRANCH_CITY_HO),0) 
	as CP_HOA_HONG_BRANCH_CITY_PB,
	x.CP_THUAN_KD_KHAC_BRANCH_CITY + ifnull((x.CP_THUAN_KD_KHAC_BRANCH_CITY_HO * y.AVG_DNCK_BRANCH_CITY / z.AVG_DNCK_BRANCH_CITY_HO),0) 
	as CP_THUAN_KD_KHAC_BRANCH_CITY_PB,
	x.CP_DT_KINH_DOANH_BRANCH_CITY + ifnull((x.CP_DT_KINH_DOANH_BRANCH_CITY_HO * y.AVG_DNCK_BRANCH_CITY / z.AVG_DNCK_BRANCH_CITY_HO),0) 
	as CP_DT_KINH_DOANH_BRANCH_CITY_PB
	from 
	(
		select z.`Tỉnh/TP` as BRANCH_CITY , x.CP_HOA_HONG_BRANCH_CITY, x.CP_THUAN_KD_KHAC_BRANCH_CITY , x.CP_DT_KINH_DOANH_BRANCH_CITY,
		y.CP_HOA_HONG_BRANCH_CITY as CP_HOA_HONG_BRANCH_CITY_HO, 
		y.CP_THUAN_KD_KHAC_BRANCH_CITY as  CP_THUAN_KD_KHAC_BRANCH_CITY_HO, 
		y.CP_DT_KINH_DOANH_BRANCH_CITY as CP_DT_KINH_DOANH_BRANCH_CITY_HO
		from CP_THUAN_HOAT_DONG_KHAC x 
		join CP_THUAN_HOAT_DONG_KHAC y on y.MA_BRANCH_CITY = '00'
		left join province_dim z on x.MA_BRANCH_CITY = z.DD 
	) x 
	left join avg_dnck_branch_city y on x.BRANCH_CITY = y.branch_city  
	left join 
	(
		select sum(AVG_DNCK_BRANCH_CITY) as AVG_DNCK_BRANCH_CITY_HO
		from avg_dnck_branch_city
	) z on 1=1 ;
	commit;
	
	-- chi phi du phong -- 
	truncate table cp_du_phong_branch_city_gl ;
	insert into cp_du_phong_branch_city_gl
	select substring(ANALYSIS_CODE,10,2) as BRANCH_CITY,
	sum(
		case
			when ACCOUNT_CODE in 
			(
				790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 
			882200000001, 882200050101, 882200020101, 882200060001,790000050101 , 882200030101
			) then AMOUNT
			else 0
		end
	) as CP_DU_PHONG
	from txn_raw_data_gl trdg 
			where ANALYSIS_CODE like 'DVS%'
			and date_format(TRANSACTION_DATE,'%Y%m') between vBegin_Monthkey and vMonthkey
			group by substring(ANALYSIS_CODE,10,2);
	-- phan bo chi phi du phong -- 
	truncate table cp_du_phong_branch_city_pb ;
	insert into cp_du_phong_branch_city_pb
	select x.branch_city ,x.cp_du_phong + ifnull(
	( x.cp_du_phong_HO * y.AVG_DNCK_DB_GROUP_2_5_TRUOC_WO / z.AVG_DNCK_DB_GROUP_2_5_TRUOC_WO_HO ),0)
	as CP_DU_PHONG_BRANCH_CITY_PHAN_BO
	from 
	(
		select z.`Tỉnh/TP` as branch_city , z.dd, x.cp_du_phong , y.cp_du_phong as cp_du_phong_HO
		from cp_du_phong_branch_city_gl x
		join cp_du_phong_branch_city_gl y on y.branch_city = '00'
		left join province_dim z on x.branch_city = z.DD  
		where x.branch_city <> '00'
	) x 
	left join AVG_DNCK_BRANCH_CITY_TRUOC_WO y on x.branch_city = y.branch_city 
	left join 
	(
		select sum(AVG_DNCK_DB_GROUP_2_5_TRUOC_WO) as AVG_DNCK_DB_GROUP_2_5_TRUOC_WO_HO
		from AVG_DNCK_BRANCH_CITY_TRUOC_WO
	) z on 1=1 ;
	commit;	
		
		
	-- TINH PHAN BO CHI PHI THUAN KDV -- 
truncate table CP_THUAN_KDV_TOAN_HO ;
insert into CP_THUAN_KDV_TOAN_HO 
select '00' as PROVINCE_CDE,
(chi_phi_von_cctg * lai_tu_the ) / (lai_toan_hang+doanh_thu_nguon_von) as cp_von_cctg_dvml,
(chi_phi_von_tt_1 * lai_tu_the ) / (lai_toan_hang+doanh_thu_nguon_von) as cp_von_tt_1_dvml,
(chi_phi_von_tt_2 * lai_tu_the ) / (lai_toan_hang+doanh_thu_nguon_von) as cp_von_tt_2_dvml
from 
(
		select 
        -- doanh thu nguon von : 
        /* 702000040001,702000040002,703000000001,703000000002,703000000003,703000000004,
        721000000041,721000000037,721000000039,721000000013,721000000014,721000000036,723000000014,
        723000000037,821000000014,821000000037,821000000039,821000000041,821000000013,821000000036,
        823000000014,823000000037,741031000001,741031000002,841000000001,841000000005,841000000004,
        701000000001,701000000002,701037000001,701037000002,701000000101 -- 
        */
        sum(
            case
                when trim(ACCOUNT_CODE) in 
                ( 702000040001,702000040002,703000000001,703000000002,703000000003,703000000004,
        721000000041,721000000037,721000000039,721000000013,721000000014,721000000036,723000000014,
        723000000037,821000000014,821000000037,821000000039,821000000041,821000000013,821000000036,
        823000000014,823000000037,741031000001,741031000002,841000000001,841000000005,841000000004,
        701000000001,701000000002,701037000001,701037000002,701000000101)
                then amount
                else 0
            end
        ) as doanh_thu_nguon_von,
        -- CP von chung chi tien gui : 803000000001 
        /* 
           khong phan bo duoc cho cac kenh , chi phi bao gom : HO , HCM , 3P , DVML ( NSM , cac vung )
           can loai HO , HCM , 3P
        */
        sum(
            case
                when trim(ACCOUNT_CODE) in ( '803000000001')
                then amount
                else 0
            end
        ) as chi_phi_von_cctg ,
        -- CP vốn TT 1 
        sum(
            case
                when trim(ACCOUNT_CODE) in ( '802000000002','802000000003','802014000001','802037000001')
                then amount
                else 0
            end
        ) as chi_phi_von_tt_1,
        -- CP vốn TT 2
        sum(
            case
                when trim(ACCOUNT_CODE) in ( '801000000001','802000000001')
                then amount
                else 0
            end
        ) as chi_phi_von_tt_2,
        -- sum voi thu nhap lai toan hang -- 
        sum(
            case
                when trim(ACCOUNT_CODE) in ( 
                -- lai trong han -- 
                '702000030002' ,'702000030001','702000030102',
                -- lai qua han -- 
                '702000030012', '702000030112',
                -- phi bao hiem -- 
                '716000000001',
                -- phi_tang_han_muc --
                '719000030002',
                -- phi wo -- 
                '719000030003','719000030103','790000030003','790000030103',
                '790000030004','790000030104','719000030004','719000030104'
                )
                then amount
                else 0
            end
        ) as lai_toan_hang
        from txn_raw_data_gl trdg 
        -- lay du lieu tu dau nam den hien tai
        where date_format(TRANSACTION_DATE,'%Y%m') between vBegin_Monthkey and vMonthkey
        and trim(ACCOUNT_CODE) not like 'Z%'
) x
left join 
(
    select 
    -- sum voi thu nhap lai toan hang -- 
    sum(
        case
            when ANALYSIS_CODE like 'DVS%' 
            and trim(ACCOUNT_CODE) in ( 
            -- lai trong han -- 
            '702000030002' ,'702000030001','702000030102',
            -- phi bao hiem -- 
            '716000000001',
            -- phi_tang_han_muc --
            '719000030002',
            -- phi wo -- 
            '719000030003','719000030103',
            '719000030004','719000030104'
            )
            then amount
            -- phí hoàn nhập dự phòng 
            -- write off : đém toàn ộ ôdư nợ gốc , lãi , phí của ông khách hàng ->  để ra ngoại bảng để theo dõi. 
            -- > hoàn lại cục trích lập dự phòng của khách hàng : 
            when ANALYSIS_CODE like 'DVS%' 
            and trim(ACCOUNT_CODE) in ('790000030004','790000030104') 
--             and TRANSACTION_DESCRIPTION NOT LIKE '%Thu - phí đã%' 
            then amount
            else 0
        end
    ) as lai_tu_the
    from txn_raw_data_gl
    -- lay du lieu tu dau nam den hien tai
    where date_format(TRANSACTION_DATE,'%Y%m') between vBegin_Monthkey and vMonthkey
    and trim(ACCOUNT_CODE) not like 'Z%'
) y on 1=1 ;

-- tinh 4 chi so tai chinh -- 
truncate table CHI_SO_TAI_CHINH_BRANCH_CITY ;
-- luu y them dieu kien rang :
-- case when mau = 0 then 0 else tu / mau end 
insert into CHI_SO_TAI_CHINH_BRANCH_CITY
select MA_BRANCH_CITY , BRANCH_CITY ,
-- CIR : tong chi phi hoat dong / tong thu nhap hoat dong  
TONG_CP_HOAT_DONG_BRANCH_CITY * -100 / 
(TONG_THU_NHAP_HD_THE_BRANCH_CITY + TONG_CP_THUAN_KDV_BRANCH_CITY + TONG_CP_THUAN_HD_KHAC_BRANCH_CITY)
as CIR,
-- MARGIN : LN TRUOC THUE / TONG DOANH THU : 
-- LNTT : tong thu nhap hoat dong + tong chi phi + chi phi du phong 
(TONG_THU_NHAP_HD_THE_BRANCH_CITY + TONG_CP_HOAT_DONG_BRANCH_CITY + TONG_CP_DU_PHONG_BRANCH_CITY) * 100 /
(TONG_THU_NHAP_HD_THE_BRANCH_CITY + TONG_DOANH_THU_KHAC_BRANCH_CITY ) as MARGIN,
-- HIEU SUAT VON : LNTT / CHI PHI THUAN KDV
(TONG_THU_NHAP_HD_THE_BRANCH_CITY + TONG_CP_HOAT_DONG_BRANCH_CITY + TONG_CP_DU_PHONG_BRANCH_CITY) * -100 /
TONG_CP_THUAN_KDV_BRANCH_CITY as HS_VON,
-- HSBQ_NHAN_SU : LNTT / SLNV TRONG TINH 
(TONG_THU_NHAP_HD_THE_BRANCH_CITY + TONG_CP_HOAT_DONG_BRANCH_CITY + TONG_CP_DU_PHONG_BRANCH_CITY) /
SLNV as HSBQ_NHAN_SU
from 
(
	select X.DD as MA_BRANCH_CITY , X.`Tỉnh/TP` as BRANCH_CITY 
	, ifnull(a1.TONG_CP_HOAT_DONG_BRANCH_CITY,0) as TONG_CP_HOAT_DONG_BRANCH_CITY
	, ifnull(a2.TONG_THU_NHAP_HD_THE_BRANCH_CITY,0) as TONG_THU_NHAP_HD_THE_BRANCH_CITY
	, ifnull(a3.tong_cp_thuan_kdv_branch_city,0) as TONG_CP_THUAN_KDV_BRANCH_CITY
	, ifnull(a4.tong_cp_thuan_hoat_dong_khac_branch_city,0) as TONG_CP_THUAN_HD_KHAC_BRANCH_CITY
	, ifnull(a5.CP_DU_PHONG_BRANCH_CITY_PHAN_BO,0) as TONG_CP_DU_PHONG_BRANCH_CITY
	, ifnull(a3.dt_nguon_von_branch_city,0) + ifnull(a4.DOANH_THU_KHAC_BRANCH_CITY,0) 
	as TONG_DOANH_THU_KHAC_BRANCH_CITY
	, ifnull(a6.SLNV,0) as SLNV
	from province_dim X 
	-- tong chi phi hoat dong
	left join tong_cp_hoat_dong_branch_city_pb a1 on x.dd = a1.MA_BRANCH_CITY
	-- thu nhap hoat dong the
	left join 
	(
		select BRANCH_CITY ,  
		( LAI_TRONG_HAN_BRANCH_CITY_PB +
		LAI_QUA_HAN_BRANCH_CITY_PB + 
		PHI_BAO_HIEM_BRANCH_CITY_PB + 
		PHI_TANG_HM_BRANCH_CITY_PB + 
		PHI_TRA_CHAM_BRANCH_CITY_PB ) as TONG_THU_NHAP_HD_THE_BRANCH_CITY
		from tong_thu_nhap_hd_the_phan_bo m
	) a2 on (X.`Tỉnh/TP` = a2.branch_city)
	-- chi phi thuan kinh doanh von 
	left join 
	(
		select branch_city ,
		-- doanh thu nguon von 
		0 as dt_nguon_von_branch_city,
		(cp_von_cctg_branch_city +
		cp_von_tt_1_branch_city +
		cp_von_tt_2_branch_city) as tong_cp_thuan_kdv_branch_city
		from cp_thuan_kdv_branch_city
	) a3 on (X.`Tỉnh/TP` = a3.branch_city)
	-- chi phi thuan hoat dong khac 
	left join 
	(
		select branch_city ,
		-- doanh thu khac : doanh thu fintech + tieu thuong + kinh doanh 
		-- fintech 
		0 + 
		-- tieu thuong 
		0 +
		CP_DT_KINH_DOANH_BRANCH_CITY_PB as DOANH_THU_KHAC_BRANCH_CITY ,
		(CP_HOA_HONG_BRANCH_CITY_PB +
		CP_THUAN_KD_KHAC_BRANCH_CITY_PB +
		CP_DT_KINH_DOANH_BRANCH_CITY_PB) as tong_cp_thuan_hoat_dong_khac_branch_city
		from cp_thuan_hoat_dong_khac_phan_bo
	) a4 on (X.`Tỉnh/TP` = a4.branch_city)
	-- chi phi du phong 
	left join cp_du_phong_branch_city_pb a5 on (X.`Tỉnh/TP` = a5.branch_city)
	left join 
	(
		select Province as branch_city , count(1) as SLNV
		from chi_so_theo_dsm_report
		group by Province 
	) a6 on (X.`Tỉnh/TP` = a6.branch_city)
	where `DD` <> '00'
) X ;
commit; 

-- do du lieu vao bang target cuoi cung -- 
truncate table rpt_fin_dsm_ranking_monthly ;
insert into rpt_fin_dsm_ranking_monthly
select a.month_key ,
a.Province , a.Staff_Code_DSM, 
a.LTN_AVG , a.rank_ltn_avg , a.PSDN_AVG , a.rank_psdn_avg , 
a.APPROVAL_RATE_AVG , a.rank_APPROVAL_RATE_AVG , a.NPL_TRUOC_WO_LUY_KE , a.rank_NPL_TRUOC_WO_LUY_KE ,
rank() over ( order by (a.rank_ltn_avg + a.rank_psdn_avg + a.rank_APPROVAL_RATE_AVG + a.rank_NPL_TRUOC_WO_LUY_KE) asc ) as RANK_PTKD,
b.CIR , b.rank_CIR , b.MARGIN , b.rank_MARGIN , b.HS_VON , b.rank_HS_VON, b.HSBQ_NHAN_SU , b.rank_HSBQ_NHAN_SU ,
rank() over ( order by (b.rank_CIR + b.rank_MARGIN + b.rank_HS_VON + b.rank_HSBQ_NHAN_SU) asc ) as RANK_FIN,
rank() over (
order by (a.rank_ltn_avg + a.rank_psdn_avg + a.rank_APPROVAL_RATE_AVG + a.rank_NPL_TRUOC_WO_LUY_KE) 
+ (b.rank_CIR + b.rank_MARGIN + b.rank_HS_VON + b.rank_HSBQ_NHAN_SU) asc
) as RANK_FINAL
from 
(
	select x.month_key ,
	x.Province , x.Staff_Code_DSM ,
	x.LTN_AVG ,
	rank() over (order by x.LTN_AVG desc) as rank_ltn_avg,
	x.PSDN_AVG ,
	rank() over (order by x.PSDN_AVG desc) as rank_psdn_avg,
	x.APPROVAL_RATE_AVG ,
	rank() over (order by x.APPROVAL_RATE_AVG desc) as rank_APPROVAL_RATE_AVG,
	x.NPL_TRUOC_WO_LUY_KE ,
	rank() over (order by x.NPL_TRUOC_WO_LUY_KE asc) as rank_NPL_TRUOC_WO_LUY_KE
	from chi_so_theo_dsm_report x  
) a 
left join 
(
	select branch_city ,
	CIR,
	rank() over (order by 
	case 
		when x.CIR < 0 then x.CIR + 10000
		else x.CIR
	end asc) as rank_CIR,
	ifnull(MARGIN,0) as MARGIN,
	rank() over (order by 
	ifnull(MARGIN,0) desc) as rank_MARGIN,
	ifnull(HS_VON,0) as HS_VON,
	rank() over (order by 
	ifnull(HS_VON,0) desc) as rank_HS_VON,
	ifnull(HSBQ_NHAN_SU,0) as HSBQ_NHAN_SU,
	rank() over (order by 
	ifnull(HSBQ_NHAN_SU,0) desc) as rank_HSBQ_NHAN_SU
	from CHI_SO_TAI_CHINH_BRANCH_CITY x
	where cir is not null 
) b on a.province  = b.branch_city ;
commit; 
	
END$$
DELIMITER ;
