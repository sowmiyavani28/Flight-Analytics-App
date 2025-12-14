"""
Flight Analytics ETL Pipeline
============================

• Fetch airport metadata
• Fetch flights (IST → UTC corrected)
• Insert flights
• Compute airport delay metrics
• Fetch aircraft metadata
• Insert aircraft data

Author: Sowmiya Sakthivel
Date: 2025-12-14
"""

# ============================================================
# IMPORTS
# ============================================================

import os
import time
from typing import Optional, Dict
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import requests
import pytz
from dotenv import load_dotenv
from db_connection import get_connection


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

API_KEY = os.getenv("RAPID_API_KEY")
API_HOST = os.getenv("API_HOST")

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

AIRPORTS = [
    "DEL", "BOM", "BLR", "HYD", "JFK", "LAX", "DXB",
    "SIN", "LHR", "CDG", "CCU", "PNQ", "GOI", "MAA", "MYQ"
]

DB_CONN = get_connection()
AIRCRAFT_REGISTRATIONS: set[str] = {'EW-456PA', 'G-STBD', 'N796JB', 'A6-EVI', 'N131NN', '9V-SCE', 'VT-ANA', '9V-SJF', 'N341NB', 'TF-ISP', 'N680VM', 'VT-SLP', 'G-YMMT', 'G-EUUC', '9H-NEE', 'VH-EBO', 'N277SY', 'EI-EAV', 'B-58505', 'N8714Q', 'N204JQ', 'N508SY', 'G-XWBE', 'VT-ISA', 'G-XWBR', 'N626NK', '9V-SKM', 'N246SY', 'G-XWBK', 'N507SY', 'JY-BAF', 'EC-LUX', 'D-AENI', 'N542LA', 'N396DN', 'N603JB', 'N899DN', 'EI-KGG', 'B-7367', 'VH-OQB', 'N333NB', 'N111ZM', '9K-CBB', 'N1200K', 'N173DZ', 'N113AN', 'N860NW', '9V-SHR', 'B-LJF', 'N657UA', 'N577DN', 'G-EUYW', 'OE-IVL', 'VT-ILP', 'VT-IQK', 'A6-EIV', 'LN-RGM', 'G-STBB', 'N4064J', 'N989SF', 'A6-EQN', 'G-TTNY', 'D-ANRJ', 'G-VLDY', 'N205HA', 'A6-ENP', 'N658QX', 'XA-ADD', 'F-GKXL', 'N535AS', 'F-GSPZ', 'N974AN', 'N8508W', 'B-LJL', 'B-1566', 'VT-HKG', 'VT-ISL', 'TC-MNV', 'N610CZ', 'XA-VRR', 'G-DBCK', 'G-VNYL', 'N997AA', 'YI-ASW', 'A6-EUP', 'N998JE', 'N505DZ', 'B-KQN', 'VT-TNI', 'VT-ILS', 'N313PQ', 'N952AK', 'C-GKQO', 'XA-VAY', 'HZ-AR24', 'D-AILW', 'A6-FEB', 'VT-NAC', 'XU-729', 'N253UP', 'N782AN', 'N652JB', 'F-GZCK', '9V-SKS', 'VH-EBG', 'A6-EGS', 'N2169J', 'N407DX', '9H-HFA', 'B-32C9', 'G-VIIN', 'N569AS', 'N305NX', 'SU-GEV', 'N4048J', 'A6-FMJ', 'A6-EGV', 'YL-CSH', 'VT-ILI', 'A6-FKI', 'OO-TCV', 'C-FIBA', 'N803AL', 'A6-BMA', 'F-HBLI', 'N536JB', 'N538DN', '9V-SFM', 'F-HRBA', 'N829AN', 'SU-GEJ', 'HL8361', 'N8525S', 'N682NK', 'A6-EEE', 'VT-ISX', 'G-VIIG', 'G-EUPO', '9V-OJA', 'G-ZBJB', 'CS-TCF', 'JA879J', 'G-TTOE', 'N301PQ', 'VT-IYD', 'A6-EEQ', 'N104NN', 'N361DN', 'N487WN', 'OE-LXD', '9V-NCE', 'ZK-OKQ', 'B-LRA', 'N8550Q', 'VT-TNN', '9V-OFI', '9V-DHE', 'XA-VAE', 'N563JB', 'N279JB', 'N29978', 'G-EUUU', 'A6-EII', 'N128HQ', 'G-EUUP', 'N501SY', 'G-EUPZ', '9V-SCN', 'N651GT', 'N301NB', 'HL8212', 'D-ABYO', 'N428YX', 'F-HBXB', 'N375NC', 'N4080J', 'N370NB', 'G-EUYM', 'N418YX', 'F-GZNE', 'A6-EPT', 'N839MH', 'A6-EUT', 'N21108', 'G-EUYT', 'C-FRQM', 'VT-CIN', 'F-GUGQ', 'G-EZUI', 'G-TTNS', 'N39415', 'N45956', 'G-EIDY', 'N986JB', 'N109NN', 'LX-LQC', 'N301DV', 'A6-EPC', 'VT-IMU', 'N557XJ', 'VT-ANY', 'VT-ISQ', 'N26902', 'VT-IMH', 'N7885A', 'VT-PPU', 'N564AS', 'B-LQH', '9M-RAC', 'VT-SQD', 'VT-ANV', '9V-SHK', 'N17322', 'A4O-MG', 'VT-ANW', 'VT-PPH', 'CN-RGZ', '9V-TRW', 'OO-SSO', 'B-32DM', 'F-GZNO', 'N37516', 'EC-MAI', 'HB-JPD', 'B-LRI', 'N69888', 'VT-PPI', '9H-CXA', 'VT-ISM', 'N937AK', 'D-AIXP', 'N941AK', 'N968JT', 'N551NW', 'G-EUUK', 'N8579Z', 'A6-ECM', 'N346FR', 'HZ-AK37', 'ET-ATR', 'A6-FMM', 'JA879A', 'A9C-ND', 'N417DX', 'B-1493', 'JA805A', 'N913JB', 'F-GSQV', 'N9013A', 'F-GKXQ', 'F-HEPH', 'G-EUPY', 'VT-TSQ', 'HL8517', 'XA-VCC', 'PK-LUW', 'N658UA', 'G-DBCG', '9V-TNF', '9V-SMP', 'N326SJ', 'EI-LRG', 'N995JL', 'N674UA', 'VT-SLA', 'N507DZ', 'OE-IMD', 'C-FSJJ', '9V-MGK', 'G-RAES', 'A6-EGH', 'N2142J', 'VT-IIQ', 'N789SK', 'VT-BDA', 'F-HBLK', 'VT-SGG', 'F-HTYQ', 'VT-ILF', 'TC-JTI', 'N705JB', 'A6-EVA', 'ZK-OKM', 'VT-JRH', 'N430SY', 'G-XLEJ', 'N27292', 'VT-EXH', 'EC-OCS', 'N846NN', '9V-SFK', 'A7-AND', 'N8803L', '7T-VKC', 'C-GVWA', 'A6-FEE', 'N882BL', 'N2084J', 'F-HBLL', 'A6-FGJ', 'N210JQ', 'N181GJ', 'VT-ILE', 'N753QS', 'N654DL', 'N239WN', '9M-AHZ', 'G-EZUN', '9V-JSK', 'F-HPNM', 'VT-SYZ', 'N205JQ', 'G-EUUG', 'VT-IFZ', 'F-HRBG', 'DQ-FAI', 'N238JQ', 'G-VIIC', 'N247JL', 'F-GZCG', 'B-2077', 'G-EUYH', '9V-MBJ', 'JA827J', 'N3142J', 'A4O-BAB', 'N550DN', 'A7-BAC', 'A6-FEW', 'N659DL', 'VT-IMB', 'PK-AZP', '9V-SCW', 'G-VNVR', 'YL-LDD', 'N3215J', 'N589JB', 'A6-EGI', 'N910DU', 'TF-AEW', 'N544DN', 'A6-EVG', 'VT-IAQ', 'G-VIID', 'N393DA', 'N722AN', 'VT-TNF', 'N8570W', 'VT-IIR', 'VT-IZV', 'HK-5365', 'PH-AXB', 'F-GKXI', 'N466CA', 'N962AV', 'G-VCRU', '9V-SCT', 'A6-ENR', 'F-HRBF', 'VT-CIM', 'JY-AZC', 'N394DA', 'D-AIEN', 'VT-ILV', 'N334DN', 'N378HA', 'A6-EEW', 'N407AN', 'G-EUUA', 'N4073J', 'VT-EXO', 'N6879R', 'N713TW', 'N702GT', 'N109JS', 'N2044J', 'N292WN', 'N8638A', 'N203JQ', 'HB-JHE', '7T-VCD', '9K-AOC', 'A6-EWB', 'G-VGEM', 'N930XJ', 'G-STBH', 'G-VSRB', 'A6-EOL', 'F-GSPQ', 'EI-LRE', 'A6-EUG', 'N624QX', 'VT-NAA', 'N518DQ', 'SP-LVP', 'N585JB', 'N827AN', 'VT-EXE', 'N438AN', 'N565NC', 'G-ZBKP', 'N969AK', 'HS-TXR', 'N911DQ', 'VT-ANQ', 'G-VIIS', 'A6-ENI', 'G-XLEI', 'N644JB', 'N582DN', 'A6-FMB', 'XA-ADG', 'N405DX', 'N283JB', 'A7-BEQ', 'EC-ODM', 'G-EUPU', 'VN-A698', 'G-ZBLK', 'N220JQ', 'VT-TQA', 'N257SY', '9XR-WX', 'A6-ENY', 'N166PQ', 'G-VPRD', 'VT-BWU', 'VT-IXR', 'N465AN', 'G-EUUR', 'G-ZBLJ', 'XA-DAO', 'N12004', 'B-30CE', 'N284FE', 'F-GSQS', 'N304RB', 'AP-BMM', 'N30913', 'F-HUVD', 'VT-ANI', 'F-HDRE', 'B-2096', '9V-SWU', '9H-SLF', '9V-SCS', 'N315SY', 'VT-TVH', 'F-GZNN', 'F-HTYM', 'F-GUGP', 'C-GWSN', 'N703JB', 'N409SY', 'N803DN', 'N726AN', 'PK-LAF', 'HZ-AR28', 'JA888A', 'G-EUOG', 'A6-EUM', 'VT-IFK', 'N842DN', 'N174DZ', '9V-SWO', 'N4074J', 'N76532', 'N432AN', 'N2029J', '9V-MBA', 'A6-EGA', 'D-AILD', '6V-ANB', 'VT-ALF', 'VT-AEM', 'N309SY', '7T-VJC', 'HL8001', 'N68061', '9V-SCF', 'N37530', 'A6-EOK', 'G-ZBLF', 'N532AS', 'HS-BBL', 'F-GSQA', 'F-HBXG', 'JY-AYT', 'F-GSPP', 'N293SY', 'PH-NXM', '9V-SCM', 'A6-EGT', 'N7835A', 'F-HTYH', 'N512FX', 'F-HRBC', 'B-7343', 'G-EUUZ', 'HZ-AS61', 'N175SY', 'N959AN', 'N487MC', 'OE-IVV', 'LN-RKR', 'JY-AYU', 'VT-IIH', 'VT-GHA', '9V-OJH', 'N27959', 'A6-FMU', 'G-TNEC', '9V-JSV', 'VT-AXW', 'VT-RTP', 'N464WN', 'G-TTND', 'N782AM', 'EC-JGS', 'HB-JLQ', 'G-DBCE', '3B-NBP', 'A6-APF', 'VT-ANM', 'G-TTNN', 'N825NW', '9V-SWS', 'N456AN', 'G-EUUV', 'N485MC', 'VH-ZNH', 'D-AIUI', 'F-HBLY', 'VT-TYC', 'N814AA', 'N373NW', 'VH-OQJ', 'N826NN', 'N462AS', 'N587JB', '9V-SHI', 'N281AK', '9V-SJI', 'D-AGWB', 'N969JT', 'N178DN', 'ZK-NZL', 'F-HZUY', 'N934AN', 'N530MM', 'F-GTAT', 'N147PQ', '9M-RAH', 'N829DN', '7T-VKA', 'OO-SBD', 'N179DN', '9V-SKZ', 'N769AV', 'EI-DEH', 'N356TX', 'HZ-NS27', 'N312NV', 'RP-C4106', 'VT-IQF', 'N544US', 'N24973', 'N399HA', 'A6-EOU', 'OE-LZO', 'SX-GRB', 'F-HBLP', 'F-HTYN', 'JA932A', 'G-NEOR', 'N67350', 'HP-9928CMP', 'N33289', 'VT-BXR', 'N513DA', 'A6-FKK', 'G-UZMI', 'G-VFAN', 'A6-EPW', 'D-AZMM', 'F-HPNN', 'N377DN', 'D-AINU', 'N925NK', 'G-EUUE', 'N39475', 'VT-ATF', 'N536DN', 'VT-ILU', 'N187DN', 'N565JB', 'N27258', 'N704FR', 'N299PQ', 'F-HEPE', 'A6-FKT', 'N73283', 'JA736J', 'YL-LDE', 'F-GSQJ', 'G-TTNT', 'G-VBZZ', 'G-VTOM', 'YL-LDW', '9M-AHE', 'OE-LZP', 'N77012', 'A6-ETI', 'HZ-AS57', 'N855NW', 'D-AJFK', 'N368UP', 'PK-LUK', 'A7-BEE', 'A5-JKW', 'N542DE', 'JA845J', 'N623UX', 'XU-878', 'XA-VRP', 'N267JB', 'A7-AMJ', 'G-DHLX', 'G-EUYF', 'YL-AAS', 'A6-EWF', 'A6-EOH', 'S2-AEQ', 'A6-FEV', 'N327NW', 'XA-VBM', 'G-NEOU', 'G-STBE', 'N145SY', 'F-HRBI', 'VT-BOM', 'N37471', '9V-SCP', 'TC-LAG', 'N933AK', '9V-MBE', 'EI-SIX', 'JA866J', 'A6-FGI', 'A6-EGJ', 'N165NN', 'N856NW', 'G-XWBO', 'VT-EXJ', 'N218JQ', '9V-TRN', 'F-HBXI', 'N781HA', 'TS-INF', 'N531JL', 'EC-NFZ', 'G-VIIF', 'TS-IMB', 'F-GRXF', 'N938AK', 'VT-TNW', 'N751AN', 'N426DZ', 'VT-TQE', 'N233JQ', 'N3065J', 'N625JB', 'N549DN', 'F-HBNB', 'VT-BXV', 'VT-ILN', 'VT-IYU', 'N2180J', 'N227JQ', 'HZ-AK43', 'A6-FKH', 'VT-IYP', 'N194DN', 'N548VL', 'G-ZBKL', 'HL7626', 'A6-EOJ', 'F-GSPL', 'G-ZBJA', 'N564JB', 'VT-IVB', 'N534JB', 'RA-73672', 'N412DX', 'F-HEPK', 'G-ZBJK', 'F-HZUM', '9M-AQN', 'VT-ANP', 'N679AW', 'B-2022', 'N930DZ', 'N723AN', '9V-SWT', 'N240SY', 'VT-IRA', 'N650FR', 'N430DX', 'VT-TSH', 'VT-ISJ', 'N428UA', 'EI-NSE', 'TC-JFH', 'D-ANCZ', 'A6-FGE', 'PK-GPW', 'N804JB', 'N738SK', 'N358DN', 'PK-AZD', 'A7-BEG', 'N831DN', 'N491AS', 'N106SY', 'PK-AZQ', 'A6-FNC', 'N319PQ', 'G-ZBKF', 'N56859', 'VT-ITL', 'B-1377', 'N37263', 'D-AIRW', 'HS-LGI', 'A6-ENM', 'G-EUYI', 'N572DT', 'G-XWBI', 'N12114', 'N225JQ', 'N837AN', 'D-AIJN', 'C-GHKW', 'TC-LLD', 'VT-IMC', 'A6-FMN', 'TC-JRS', 'VT-PPW', 'N189DN', 'N27304', 'A6-FKM', 'D-ABYR', 'VN-A544', 'PK-GPT', 'F-HBLX', 'A6-EQM', 'F-GSPG', 'G-EUYJ', 'N977JE', 'N786UA', 'F-HPNK', 'C-FVND', 'G-EUUB', 'N117HQ', 'G-EUOF', 'A6-EOP', 'F-HZUA', 'YL-ABP', 'N29985', 'G-EUYP', 'N533DT', 'VT-TNH', 'A6-FES', 'VT-TQH', 'A6-EEA', 'N601CN', 'D-AINK', '9V-SJG', 'G-VAHH', 'OH-LZP', 'N597JB', 'VT-TYB', 'EI-DEE', 'N410AN', 'G-ZBJJ', 'VT-IFV', 'VT-MLE', 'N468AN', 'EI-SIM', 'G-XWBB', 'B-20C6', 'G-EZTT', 'F-GSPU', 'VT-ICC', 'G-STBI', 'VT-IZQ', 'N27509', 'A6-FKB', 'G-EUPP', 'F-GTAY', 'EI-LRA', 'G-TTSA', 'OH-LZN', 'N337QT', 'CC-BBF', 'N891DN', 'F-GZNT', 'VT-IMD', 'N729AN', 'G-XLEF', 'OE-LKM', 'A6-ENV', 'G-TNEA', 'N344FR', 'N3104J', 'N202JQ', 'A6-FEQ', 'VT-TNC', 'A4O-BAE', 'VT-ILH', 'A6-EES', 'N432UA', '9V-SMO', 'VT-TNP', 'B-16716', 'N8697C', 'A6-FMR', 'HZ-NS53', 'TF-ICT', '9M-AQQ', 'N517SY', 'G-STBL', 'A6-FED', 'B-LRR', 'A6-EGO', 'F-GRHR', 'A6-FKL', 'VT-EXT', 'HP-9928', 'N672UA', 'N285AK', 'N804AW', '9V-SHU', 'N685UA', '9V-SCD', 'G-VLIB', 'OE-ICD', 'VT-CIG', 'TC-LGU', 'N750AN', 'VT-BXU', 'S2-AKG', '7T-VKF', 'HZ-AS74', 'F-GZNI', 'F-GSQN', 'B-KPD', 'VT-IFS', '7T-VJS', 'VT-IFQ', 'N319US', 'N3132J', 'VT-ILR', 'VT-JRE', '9V-MBC', 'VT-IIF', 'N702NK', 'VT-IXS', 'N12020', 'A6-BLA', 'N17550', 'F-GUGR', 'F-GSQR', 'N7822A', 'CC-CXH', 'CN-MAY', 'N420YX', 'N413DX', 'N915AK', 'N17104', 'G-EUUN', 'TC-JIO', 'CS-TVF', 'N956AV', 'PK-LUU', 'B-302J', 'F-HEPD', 'VT-GHD', 'YL-LDT', 'N448AN', 'G-TTNP', 'A6-FKS', '9V-SJB', 'OE-ICP', 'N314PQ', 'N336RU', 'B-2003', 'N923JB', 'OH-LWD', 'EI-SIZ', '9M-RAP', 'G-ZBLH', 'N307AZ', 'N760AN', 'N290AK', 'RP-C8782', 'N8701Q', 'N329MS', 'XY-ALK', 'G-VELJ', 'A6-FML', 'N1604R', 'VT-ANS', 'N508DA', '9V-SCR', 'EC-JZM', 'N510DE', 'F-GSQB', 'N230HA', 'N719AN', 'N570AS', '9V-SCU', 'VT-ISR', 'N107HQ', 'F-HBXM', 'N840MH', 'N528FE', 'SX-NEL', '9H-SLH', 'B-16728', 'CN-NMS', 'N306PQ', 'G-UZHV', 'PK-MYV', 'F-HTYI', 'F-GZCD', 'HL7644', 'N106NN', 'B-5327', 'A6-EVO', 'N522DA', 'XU-727', 'F-GTAQ', 'N546DN', 'N548DN', 'PH-BXW', 'VT-TGG', 'N2038J', 'OE-INP', 'A6-EPY', 'N818NN', 'F-HBLQ', 'VT-ANH', 'A6-EGM', 'B-16781', 'PK-AZA', 'N813AN', 'F-HBLM', 'N520SY', 'VT-IIM', 'D-ABGJ', 'VT-ILC', 'VT-IZC', 'B-32DL', 'HB-IOO', 'N792AV', 'N3756', 'N348TU', 'F-HBLO', 'A6-EIM', 'N428AA', 'VT-TVC', 'G-EUPN', 'G-EUUS', 'F-GSQM', 'TC-JJL', 'N619FR', 'VT-IJX', 'A6-EED', 'F-HUVB', 'N413YX', 'A6-EOQ', 'A6-EEL', 'D-AENH', 'S2-AJS', 'EI-EJG', 'N850NN', 'OE-IWF', '9H-SLI', 'A7-BEU', 'PH-NXR', 'N580DT', 'N989JT', 'N819DN', 'N962JT', 'N579DT', 'B-16731', 'A7-APD', 'VT-EXQ', 'G-STBA', 'N154UW', 'N8757L', 'N830NW', 'VT-AEH', 'VT-EXF', 'VT-TNX', 'N507JT', 'N414SY', 'G-EZWY', 'VT-ISC', 'VT-BWF', 'G-EUYU', 'G-VDOT', 'N810DN', 'N163AA', 'N132HQ', 'N814DN', '9K-AOI', 'F-GZNB', 'G-EUYE', 'B-2094', 'N3185J', 'N185UW', 'EC-NTA', 'F-GZNR', 'N425DX', 'A6-EOY', 'N805NN', 'N937JB', 'EI-NSA', 'VT-ATV', 'B-LXD', 'G-XLEC', '9M-AQH', 'A6-EOF', 'N360HA', 'N909NN', 'N8677A', 'B-16733', 'N972JT', 'G-ZBJH', 'G-EUYO', 'VT-ISP', 'N639JB', 'PH-BVN', 'G-VRIF', 'G-EZOF', 'N306PB', 'N37464', 'N9642F', 'N931NK', 'F-HIQH', 'D-AIRR', 'VT-TNY', 'VT-IJJ', '9V-TRM', '9V-SCQ', 'N2105J', 'F-HTYE', 'N57864', 'B-2026', 'N103DY', 'N101NN', 'F-GSPA', 'N654NK', 'N809JB', 'N24519', 'N116AN', 'D-AINE', '9V-SHT', 'G-EUYR', 'N721AN', 'EI-DTA', 'CN-ROJ', 'G-VZIG', 'EI-SIF', 'EI-NSD', 'N124HQ', 'VT-IIA', 'VT-TNK', 'N717TW', 'G-EUYS', 'F-HBXF', 'A6-FMS', 'N773UA', 'TC-LJT', 'N108HQ', 'N820DN', 'N853GT', 'N3247J', 'N946WN', 'D-AIUA', 'VT-IIL', 'VT-EXA', 'N363NB', 'A6-FMG', 'OH-LKO', 'C-FITW', 'N466AN', 'N545XJ', '9V-SCA', 'N4062J', '9M-FYC', 'A6-FGC', 'CS-TJI', 'A6-EQP', 'N803AK', 'G-EUYX', 'VT-EXV', 'N7746C', 'G-EUYL', '9M-LNP', 'A6-ENB', 'ET-AUO', 'VT-ALT', 'C-FVLZ', 'VH-OQD', 'UK32022', '9M-AQE', '5Y-KZF', 'N125DU', 'C-GOIO', 'VT-TQC', 'N8933Q', 'G-YMMS', '9V-NCA', 'EC-NDN', 'VT-CIO', 'N88AQ', 'N8315C', 'HZ-AR25', 'F-HZUO', 'N179SY', 'A6-FKR', 'VT-RTS', 'A6-FKJ', 'G-TTOB', 'N778AN', 'S2-AKD', 'N874AN', 'N991AN', 'N3115J', 'N85374', 'G-VOOH', 'VT-CIE', 'A6-EIY', 'OE-LQA', 'N76526', 'N220CY', '9V-NCD', 'N292NN', '9V-SNC', 'N504SY', 'G-ZBLG', 'F-GMZC', 'N3162J', 'A6-ENF', 'G-EUUY', 'N712SK', 'VH-OYP', 'G-DBCJ', 'N143DU', 'G-VWOO', 'VT-IVX', 'F-HUVI', '9V-JSJ', '9V-MBN', 'VT-CIQ', 'G-VIIL', 'VT-TNB', 'VT-IZE', 'N708SK', 'N720FR', '9V-JSN', 'VT-EXI', '9M-MXA', 'F-GSQH', 'OE-LQL', 'N29981', 'N235JQ', 'A6-EUZ', 'N395DZ', 'N720AL', 'N543DE', '9M-MAB', 'N178SY', 'TC-JJY', 'N265AK', 'PK-GFQ', 'B-KPX', 'EW-544PA', 'N215BZ', 'N123QA', 'JA892A', 'N127HQ', 'T7-ME5', 'A6-FNA', 'G-TTNZ', 'VT-ILW', 'N712TW', 'N420AN', 'N37538', 'N423YX', 'N377NW', '9V-MBB', 'F-HEPJ', 'VH-ZNA', 'CC-BMA', '9V-TRX', 'N1603', 'N753US', 'N530AS', 'A6-EBM', 'N828MH', 'G-EUUH', 'N389FX', 'LN-127MJ', 'N947AN', 'A6-EDO', 'VT-TQK', 'G-YMMK', 'N301PA', 'N180DN', 'N523UW', 'N120HQ', 'N328TC', 'B-1428', 'C-GKUG', 'OK-TVL', 'N836UA', 'VT-ILG', '9V-SMF', 'C-GJFZ', 'N804AK', 'EI-HXF', 'F-HTYL', 'OK-JRS', 'VT-IYH', '9V-SGG', 'VT-JRA', 'HZ-AK40', 'N181UW', 'N993JE', 'N492AS', 'A6-EQK', '9V-JSR', 'HL7620', 'VT-IMI', 'N987JT', 'G-TTNJ', 'C-GUBD', 'N562JB', 'D-AINH', 'HZ-NS23', 'N24980', 'N429AN', 'N566AS', 'N110AN', 'HL8521', 'G-VIIJ', 'N521DT', 'N730AN', 'B-32A8', 'F-HMRF', 'F-HBNG', 'N556AS', 'OE-IAJ', 'N7824A', 'G-NEOT', '9M-FYJ', 'N153AN', 'EC-MJC', 'A6-ECG', 'A6-FNB', 'G-UZLF', '9V-SWI', 'A4O-SG', 'N857RW', 'N198DN', 'N147FE', 'HS-BBX', 'N365DN', 'D-AINB', 'N868DN', 'A6-FGH', 'N635JB', 'F-HTYO', 'B-16789', 'N27958', 'VT-IFL', 'A6-FEJ', '9V-MGE', 'VT-BXW', 'TS-INR', 'N212JQ', 'N621FE', 'B-5318', 'G-TTNC', 'G-ZBJG', 'G-LMTA', 'VT-IXZ', 'VT-PPL', 'N341NW', 'G-TTNF', 'N344TS', 'N181PQ', 'N17105', 'N926WN', 'N24988', 'VT-AEN', 'N849DN', 'VT-GHF', 'G-VIIA', '9V-SKP', 'VT-IUP', 'OK-EYA', 'YI-ASN', '9M-AQB', 'HB-JCT', 'SU-GDO', 'N908DN', 'F-GKXV', '9M-LCD', 'VT-RKE', 'A6-FET', '9V-TRV', 'VT-IIS', 'N101DQ', 'N931AN', 'VT-IAR', 'G-ZBKR', 'VT-BWR', 'N914WN', 'F-HZUP', 'VT-TSN', 'N283AK', 'G-YMMN', 'VT-TNM', 'G-NEOV', 'N172DN', 'B-222K', 'N658JB', 'G-NEOX', 'EC-NSC', '9V-SHH', 'B-KPR', 'G-TTNK', 'G-VJAM', 'F-HPNO', 'A6-ECT', 'G-TTNR', 'N243JQ', 'JA798A', 'N102NN', 'TC-JTP', 'HP-9926CMP', 'N369NB', 'N597AS', 'HZ-AQ28', 'N637FR', '9K-CAL', '9M-MAC', 'N834MH', 'A6-ENO', 'VT-BWG', 'A6-EPN', 'VT-SCR', '9XR-WN', 'N419WN', 'N324SY', 'F-HUVJ', 'HZ-AQ11', '9M-MXE', 'N947QS', 'N532DN', 'N966WN', 'B-LRN', 'VT-IIP', 'N931WN', 'N127MJ', 'YI-ASZ', 'HS-TTB', 'N434AS', 'VT-ALQ', 'A6-EGP', 'A6-ECJ', 'N816NW', 'G-MIDT', 'HP-1729CMP', 'VT-IFR', 'A6-FEU', 'F-HPNC', 'N3232J', 'G-EUYK', 'N988NN', 'G-ZBJC', 'OE-IVI', '9V-TRL', 'N835MH', 'N665JB', 'C-GPTS', 'N952XV', 'HB-JDA', 'N118DY', '9V-SKQ', '9V-SHC', 'VT-ANR', 'N122DU', 'F-GZCM', 'N831AA', 'N254SY', 'N186DN', 'HZ-AK27', 'G-ZBKC', 'D-AIMK', 'N14228', 'F-HBXD', 'F-HUVF', 'VN-A899', '9V-MBM', 'N8872Q', 'C-GTSW', 'A6-EOZ', 'N3746H', 'VT-JRF', 'N418DX', 'G-ZBKE', 'N959NN', 'G-VIIB', 'N13014', 'N9018E', 'A6-EEB', 'G-ZBKA', 'F-HTYK', 'A6-ECK', 'ER-00009', 'N318AS', 'N539DN', 'A6-FMY', 'VT-IYV', 'SU-BVJ', 'VT-EDE', 'N370HA', 'N409YX', 'TC-SOT', 'VT-ANC', 'TC-MKD', 'F-GZNG', 'B-1297', 'A6-EON', 'A6-ECF', 'TC-LPB', 'VT-TVD', 'N181SY', 'VT-SLG', 'C-FAJA', 'N745CK', 'VT-ILT', 'VT-ILZ', 'F-HUVA', 'G-EUPR', 'N509SY', 'N573DT', '9V-SHE', 'B-18652', 'G-YMMP', 'A6-APH', 'F-GZNU', 'G-VJAZ', 'VT-ALH', 'A6-ECH', 'N781UA', 'N368NW', 'N967JT', 'N581AS', 'N852NW', 'VT-TVG', 'G-EUUJ', 'EI-NSC', 'PK-AZX', 'N3203J', 'G-YMMH', 'A6-FEK', 'N311DN', 'RP-C9925', 'N994NK', 'A6-EBY', 'A6-EOI', 'A4O-ME', 'A6-FPA', 'N8804L', 'VT-TQL', 'N76269', '9V-SHV', 'N429DX', 'VH-EBC', 'A6-EGB', '9V-TRI', 'N510SY', 'N24514', '9V-MBF', 'N436AN', 'XA-ADU', 'G-EUPD', 'N431UA', 'N406DX', 'N124AA', 'N27908', '9V-SME', 'OH-LKH', 'A6-ECO', 'B-329N', 'N457AM', 'N133SY', 'A6-ENQ', 'G-EUUW', 'A6-ECS', 'N793AV', 'G-VPOP', 'N400SY', '9V-SHF', 'N997JL', 'N75433', 'N903JB', '9V-SWY', 'N347TT', 'A6-FQB', 'N261FE', 'G-VTEA', 'F-GTAS', 'N524VL', 'A6-EPD', 'N7820L', 'N78004', 'CC-BGR', 'N853JS', 'N120DN', 'JY-BAB', 'JA03WJ', 'EC-MBY', 'HS-TKR', 'B-6511', 'F-HZUE', 'N221JQ', 'A6-EWE', 'VT-ITA', 'N980JT', '9V-MGM', 'N27213', 'N767CK', 'N447UA', '9H-CXF', 'A6-EEI', 'VT-JRT', 'F-HBXC', '9V-OJE', 'VT-IAS', 'N119FE', 'G-DBCH', 'VT-TNV', 'EI-LRF', 'SP-LVD', 'D-AINQ', 'A6-EUI', 'G-STBM', 'F-HZUX', '9V-TRO', 'VT-IVE', 'G-ZBKG', 'N461SW', 'N369NW', 'A6-EUU', 'N793JB', 'N583JB', 'A6-ECQ', 'N603UX', '9V-SCK', 'B-LRC', 'F-HTYS', 'N408YX', 'G-DBCB', 'F-GSQO', 'N323RM', 'N772AN', 'HB-JNK', 'N514DN', 'N642NK', 'N656JB', 'OK-TSD', 'A6-EDM', 'N17015', 'F-HBLV', 'N828JB', 'B-2025', 'VT-IYS', 'A7-BAZ', 'OE-IJH', 'N182GJ', 'B-5476', 'N364UP', 'VT-IAN', 'HZ-AK26', 'VT-IYT', 'VT-TSD', 'UK78703', 'XA-VLU', 'N324NB', 'VT-IYZ', 'VN-A612', 'N146PQ', 'N961NK', 'N231JQ', 'OE-INO', 'N131SY', 'F-HEPA', 'D-AINV', 'N309PC', 'VT-TNR', 'N3062J', 'VT-IJZ', 'VT-AER', '9V-JSO', 'XA-SRA', 'VT-EXD', 'B-32AV', 'N594JB', 'TC-JJH', 'A6-FMF', '9V-SHJ', 'VT-TNS', 'PH-HXC', 'A6-EWD', 'D-AIZX', 'N119DU', 'N7827A', 'N823AN', 'C-FIUR', 'YL-LDU', 'N913AN', 'EI-NSB', 'C-FJNX', '9V-SKT', 'N104HQ', '9V-SHB', 'A9C-FI', 'N805JB', 'VT-ISS', 'N964JT', 'F-HBLH', 'VT-ISY', '9V-SGA', 'G-VEYR', 'G-XWBC', 'EC-MHA', 'N618JB', 'N3157J', '9H-DRA', 'N961JT', 'A7-ALH', '9V-SWG', 'TC-LPI', '9K-AOL', 'N715CK', 'N434AN', '9V-NCJ', 'D-AEAD', 'VT-TSO', 'YR-BGL', 'F-HPNI', 'N501GJ', 'G-TTNX', 'F-GTAM', 'G-EUYY', 'A6-EVL', 'A6-EUA', 'B-5532', 'D-AIUO', 'N8855Q', 'N630NK', 'G-DBCC', 'N112AN', 'F-HBLN', 'N106HQ', 'F-GKXO', 'VT-JRB', 'N948JB', 'VT-TNE', '9V-SHO', 'F-HIQE', 'N329NB', 'N183AM', 'G-UZMD', 'N2039J', '9V-MBG', 'N3749D', 'N337FX', 'A6-FEY', 'G-EUYV', 'OO-SBA', 'CS-TVE', 'N451AN', 'A9C-FJ', 'VN-A644', 'CS-TKP', 'N413AS', 'F-GSQF', 'N947JB', 'N217JQ', 'N531DA', '9Y-ANT', 'N542US', 'N920AK', 'F-GSPJ', 'A6-EOC', 'G-ZBKS', 'N8861Q', 'N757AN', 'HZ-AQ19', 'N66831', 'A6-EGR', 'VT-PPQ', 'N924AK', 'G-VIIW', 'OE-IVS', 'A6-FEG', 'VT-PPM', '9V-OFH', 'N842FD', 'F-HBLC', 'VT-IJR', 'A6-EUK', 'A7-APF', 'G-ZBLB', 'N656NK', 'LX-LGG', 'VT-AEP', 'N791SK', 'N960NK', 'A6-EVC', '9M-MLJ', '9H-TJE', 'N316SE', 'G-TTNI', 'N587FX', 'N819NW', 'G-ZBKI', 'VT-AIX', 'N523AR', 'N421LV', 'N517DZ', 'EI-HXD', 'N879FD', 'G-VIIE', 'N668UA', 'AP-BLS', 'JA740J', 'N997NN', '9V-OJD', 'G-VIIM', 'F-GKXP', 'F-GZNC', 'F-HTYD', 'N12021', 'CN-RGY', 'C-GITU', 'A6-EGU', 'PK-GNR', 'A6-EPB', 'VN-A868', 'N419DX', 'N815AA', 'VT-IUF', 'N776DE', 'B-208S', 'B-32G9', 'ES-SAD', 'G-TTNB', 'N606LR', 'HP-9905CMP', 'A6-ECZ', 'N323JB', 'N759AN', 'CC-CXG', 'VT-RED', 'N77571', '9V-OFK', 'A6-FKP', 'G-SAJK', 'VT-PPX', 'G-YMMI', 'N574DZ', 'N529JB', 'HS-PGX', 'F-HTYB', 'F-HZUL', 'N901WN', 'N435AN', 'YR-BGF', 'D-AIHU', 'A6-ENK', 'N455WN', 'VT-IML', 'VT-IJY', 'G-EUYG', 'N638JB', 'EC-JRE', 'VT-ISW', 'N351FR', 'F-HBLD', 'F-GKXY', 'JA788A', 'N423AN', 'B-LDT', 'N19986', 'N934AK', 'N852SY', 'N481AS', 'VT-IFI', 'VT-RKC', 'N545DE', 'G-TNEB', 'D-AIBB', 'B-18776', 'A6-END', 'N7867A', 'TC-JJZ', 'G-EUUX', 'F-HBLR', 'JA873A', 'N8691A', '9V-SMN', 'B-302T', 'N57016', '5Y-KYF', 'N213JQ', 'N653RW', 'N255WN', 'F-GSPY', 'N2156J', 'A6-FEX', 'EI-TYA', 'HZ-AK39', 'G-EUYA', 'N374DX', 'PH-BXZ', 'CN-ROT', 'N827MH', 'N665UA', 'VT-EXM', 'N61886', 'F-GSPO', 'N206UA', 'HB-JCJ', 'HZ-NS39', 'A6-FMO', 'G-VRNB', 'N3118J', '9H-TJF', 'N901XJ', 'N554JB', 'SX-NAF', 'A6-EDZ', '7T-VJY', 'N486WN', 'F-HUVK', 'C-FZUB', 'F-GRHY', 'N707TW', 'F-HEPI', 'N572DZ', 'F-HBXH', '9V-SCO', 'N659NK', 'N29124', 'N324SH', 'A4O-MA', 'B-KPZ', 'N975JT', 'N520DE', 'N897FD', 'YU-APS', 'G-VEVE', 'A6-FMI', 'C-FIUL', 'VT-SLC', 'N935JB', 'OD-MEB', 'G-VBEL', 'N651FR', 'N3139J', 'N980AN', 'N7868K', 'G-TTNL', 'EI-DVH', 'N17126', 'VT-IZF', 'PH-BKM', 'A9C-FE', 'G-ZBJM', 'N172DZ', 'HS-THL', 'F-HZUR', 'VT-EDC', 'N967AN', 'A6-EPH', '9V-SHN', 'N914DU', 'N404AN', 'TC-LGL', 'N985JT', '9V-MBD', 'HS-THR', 'N57286', 'G-ZBKN', 'N725AN', 'VT-ATG', 'N831SY', 'N341DN', 'N415AN', 'HL7636', 'TC-LJC', 'G-NEOY', 'TF-ICD', 'HZ-AK23', 'N431WN', 'A6-ETS', 'TF-ICJ', 'N134EV', 'N19136', 'G-STBN', 'N4076J', 'G-XWBF', 'A6-FPC', 'VT-KOC', 'N371DA', 'A6-EQC', 'A6-EPF', 'VT-ALU', 'B-1308', 'SX-NAL', '9V-SMI', 'A6-EQH', 'OH-LTS', '9K-CAW', 'OE-LBL', 'F-HEPF', 'S2-AHV', 'N513DZ', 'VT-TQB', 'G-TNEE', '9V-TRU', 'VT-ANG', 'G-ZBLA', 'F-HBLZ', 'EC-LOB', 'G-ZBLE', 'N526DE', 'N422YX', 'VT-TVJ', 'JA874J', 'N202SY', '9V-OJG', 'G-TTNA', 'VT-ATE', 'C-FIUV', 'A6-EPI', 'N5827K', 'G-YMMU', 'N203BZ', 'VT-IPC', 'B-8971', 'VT-TNU', 'A6-FPD', 'F-GZNH', 'A6-EGZ', 'N452UA', 'N255SY', 'VT-ILO', 'N970BG', 'HL7203', 'VT-IXW', 'N34460', 'N3754A', 'F-HRBD', 'HZ-ARG', 'F-HBXE', 'F-GSPI', 'N219UA', 'N732AN', 'N107NN', 'G-EUYN', 'A6-EQE', 'HB-JHI', 'N766AV', 'N8582Z', 'N138SY', 'OE-LAY', 'A6-EOG', 'F-HBXJ', 'VT-IZU', 'N486AS', 'N750AX', 'HL7619', 'N915JX', 'N250SY', 'F-OMUA', 'N517DN', 'N652MK', 'N945AN', 'N412YX', 'F-HTYA', 'VT-ISZ', 'VT-TVI', '5Y-KZD', 'OE-ICI', 'G-VMAP', 'A6-EQA', 'VT-RKH', 'EI-KEB', 'F-HUVM', 'VT-CIH', '9K-AKM', 'A6-EQJ', 'G-XWBP', 'C-FRSI', 'G-VBOB', 'N535DN', 'N4058J', 'VT-ILB', 'F-GTAK', 'VT-TQG', 'N3149J', 'F-HPNA', 'N853NW', 'A6-EVB', 'G-YMMO', '9M-MXN', 'N327DN', 'N37506', 'YU-APL', 'DQ-FJT', 'F-GUGM', 'F-HZUB', 'HB-JHF', 'N402SY', 'G-XWBN', 'CN-ROP', 'N481WN', 'G-TTNW', 'JA887A', 'N161UW', 'A6-APG', 'AP-EDH', 'A6-EQD', 'N992NK', 'SE-RON', 'N996JL', 'G-STBO', 'A7-BEV', 'N288WN', '9V-SCZ', 'G-EUPW', 'C-GKXS', 'N284AK', 'B-HLQ', 'OH-LKG', 'OE-IVU', 'F-GRHV', 'N720NK', 'HL8382', 'N830MH', 'CS-TJM', 'F-GRXK', 'A6-EQF', 'N336DX', 'PH-BXI', 'G-DBCA', 'HZ-AQ17', 'A6-FGA', 'A6-EPS', 'G-EZDJ', 'VT-TSP', 'A6-EEO', 'N137WS', 'VT-SQE', 'N201JQ', 'G-VKSS', '4K-AZ81', 'N282AK', 'N501DA', 'N430AN', 'G-EUUF', 'A6-EGQ', 'F-GZNA', 'VQ-BBM', 'N452PA', 'VT-IVZ', 'N38955', 'EI-SIC', 'A6-FKC', 'SP-LVQ', 'F-HUVG', 'VN-A326', 'N175FE', 'N919NK', 'VT-ATD', 'A6-ECY', 'G-EUUL', 'F-HZUS', 'VT-IUM', '7T-VKH', 'HZ-AS54', 'VT-IME', 'F-GRHZ', 'N556UW', '9V-JSU', 'HZ-NS29', 'N579JB', 'N331QS', 'F-GKXS', 'N178DZ', 'N922DZ', '9V-SJC', 'XA-VOC', 'TC-LKB', 'F-HTYT', 'N325SY', 'G-TTNV', 'N187GJ', 'VT-GHE', 'A6-EUO', '9V-SFP', 'N444UW', 'N37018', 'N449AN', '9Y-GRN', 'N920AV', 'VT-ILQ', 'N66814', '9V-MGN', 'OE-LKO', '9V-TRP', 'F-GZND', 'EI-KGH', 'N802NW', 'N494AS', '9V-SMA', 'F-GSQK', '9M-RAL', 'VT-IUH', '9V-OFG', 'A9C-XB', 'F-GKXN', 'G-EUUO', 'B-1165', 'ZK-OKN', 'N380DA', 'N528DE', 'SX-NAC', 'G-NEOZ', '9V-TRK', 'F-GSPE', 'N8704Q', 'N713CK', 'LZ-PAR', 'N8663A', 'N406FX', 'EI-KGA', 'VT-IJM', 'N730SK', 'TC-JOG', 'HP-9921CMP', 'N826MH', 'F-HIXA', 'VT-ALR', 'JA04WJ', 'F-HZUF', 'OO-SFJ', 'C-FSKZ', 'G-UZLM', '9V-SHQ', 'VT-TNQ', 'N162SY', 'HS-BBI', 'VT-EXR', 'B-18915', 'F-HPNJ', 'N332NB', '9V-SWZ', 'CC-BGW', '9V-MBK', 'N273AK', 'N666UA', 'N857NW', 'PH-BKG', 'VT-JRI', 'N413SY', 'G-VRAY', 'A6-FMW', 'N4022J', 'G-ZBJF', 'C-FIVW', '9V-SWK', 'HB-JPA', '9V-JSL', 'G-STBF', 'B-18919', 'F-HPNF', 'F-HTYP', 'F-GSQL', 'G-XWBA', 'N825MH', 'A6-FKD', 'A6-ECX', '9V-SNA', 'N584NW', '9V-SCY', 'N662UA', '9V-SHL', 'N957JB', 'G-EUUI', 'N8931L', 'G-ZBKM', 'A6-FMA', 'N932AK', 'G-NEOW', 'F-GKXT', 'A6-EVP', '9A-CAE', 'HB-JCN', 'N177DZ', 'N261SY', 'F-HZUG', 'A6-FEM', '9V-OFC', 'VT-IFN', 'HL8045', 'ES-SAZ', 'F-HPNL', 'N929JB', 'F-GTAJ', 'B-32CA', 'N904AV', 'VT-AXT', '9V-SJA', '4R-ALN', 'VT-TNJ', 'F-GKXM', 'F-GMZD', 'YL-LDF', 'B-LJE', 'C-GPWG', 'N452AN', 'YL-CSB', 'G-XLEG', 'LZ-LON', 'B-1166', 'N403SY', 'N176DZ', 'A6-EPL', 'F-OTOA', 'TC-MKC', 'C-FKSV', 'N852QS', 'A6-EPQ', 'VT-IRD', 'F-GZNP', 'N184GJ', 'N965WN', 'VT-ANT', 'N179UW', 'N316NB', 'OE-IJV', 'N175DN', 'N177DN', 'D-AINM', 'VT-TQF', 'N624FR', 'G-EUOE', 'A6-ENW', 'N390CM', 'N337JB', 'N47505', 'D-AING', 'N970NK', 'D-AIJA', 'VT-ILL', 'HB-JND', 'A6-FKG', 'G-XWBS', 'N223JQ', 'N836MH', 'N510JB', 'A6-EWC', 'N857NN', 'VT-SQC', 'N648JB', 'N836SY', 'N44522', 'C-FUJA', 'VT-IAX', 'B-16730', 'G-VWAG', 'A6-EGX', 'N2157J', 'A6-FMX', 'VT-ALJ', 'VT-IUB', 'VT-IJB', 'EI-HHN', 'N3156J', 'N3023J', 'A6-EEK', 'N86375', '9V-SHG', 'JA742J', 'N306RC', 'N281WN', 'G-VDIA', 'G-EZWX', 'F-GSQD', 'VT-IAO', 'G-EUUD', 'N563AS', 'SU-GFO', 'N402YX', '9V-MGL', 'N837MH', 'N8869L', 'VT-EDF', 'N183SY', 'G-TTNM', 'VT-ITB', 'N418QS', 'XY-ALB', 'F-HBLE', 'G-EUPK', '9V-SNB', 'XA-AMX', 'A6-FPB', 'N190DN', 'G-ZBJD', 'N844MH', 'VT-IIK', 'VT-SXA', 'N114SY', '9V-SKR', 'N506DA', 'N538UW', 'VH-ZNJ', 'VT-TZD', 'F-GRHT', 'B-2006', 'OD-M10', 'G-ZBLI', 'N851NW', 'N8634A', 'N362NB', 'VT-ISO', 'SP-LNI', '9H-NEC', 'VT-IHZ', 'N377DE', 'F-GSPX', 'A6-EIA', 'A6-ENU', 'G-XLEH', '9H-SLD', 'F-HIXB', 'VT-TVA', 'JA735J', 'OE-LZR', 'F-HTYR', 'VT-GHK', 'JA771F', 'F-HPNH', 'CN-RGC', 'N445UP', 'VT-PPV', 'N644QX', 'D-AINI', 'N807DN', 'VT-IJA', 'G-EUYD', 'G-EZTL', 'B-30ED', 'C-FCQD', 'N337FR', 'N292PQ', '9V-SCB', 'RP-C9916', 'G-TTNU', 'HZ-NS36', 'F-HZFM', 'OD-MEA', 'N527DE', 'VT-PPT', 'N1605', 'N213SY', 'A6-ENN', 'N447AN', 'N212WN', 'SU-GDN', 'VT-EXC', 'N254WN', 'RP-C4141', '9V-NCI', 'N813NW', 'HZ-AR27', 'A6-EIH', 'VT-ILJ', 'N752AN', 'HP-1839', 'XA-JSO', 'OE-ICK', 'TC-SPC', 'G-EZGR', 'A6-ENE', 'G-VIIY', 'N412SY', '9V-NCB', '9V-SHS', 'F-GKXZ', 'VN-A632', 'A6-FMC', 'G-XWBL', '9V-NCC', 'G-XLEL', 'A6-MAX', 'F-GZCE', 'N846FD', 'A6-ECW', 'N244JQ', 'N117AN', '9V-SCL', 'RP-C7773', 'A6-EEH', 'D-AENG', 'N519SY', 'HZ-AS68', 'N316SY', 'VT-ILD', 'A6-EGK', 'F-GZCA', 'N850FD', 'VT-RTO', 'N352DN', 'A6-EVK', 'N832MH', 'D-ABGN', 'D-AIVB', '9V-MBH', 'PH-AXA', 'N3195J', 'VT-EXL', 'A6-FPE', 'N774AN', 'PH-NXC', 'N357PV', '9M-RAG', 'F-GMZE', 'VT-VTZ', 'N17303', 'N552AS', 'C-GUDO', 'N272PQ', 'N990AN', 'N690DL', '9M-LNV', 'VT-IYX', 'N568UW', 'A9C-CC', 'F-HZUJ', 'HZ-AR32', 'D-AIXI', 'C-FEKI', '9V-SFN', 'VT-KTM', 'B-20EC', 'N923SW', 'N832AA', '9V-JSI', 'N800AN', 'G-EUPJ', 'G-EUUT', 'F-ONET', 'G-EZUS', 'N575DZ', 'VT-AEE', 'N734AR', 'A6-ENZ', 'N405KZ', 'N619UX', 'N37507', 'JA15KZ', 'G-EUOA', 'N583DT', 'N514SY', 'G-NEOS', 'EI-SIH', 'VT-ANU', '9M-MXW', 'F-GKXH', 'SU-GDL'}

# Create tables
with open("schema.sql", "r") as f:
    DB_CONN.executescript(f.read())


# ============================================================
# AIRPORT FUNCTIONS
# ============================================================

def fetch_airport(iata_code: str) -> Dict:
    """Fetch airport metadata."""
    url = f"https://{API_HOST}/airports/iata/{iata_code}"
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.json()


def insert_airport(conn, airport: Dict) -> None:
    """Insert airport into database."""
    cursor = conn.cursor()

    country = airport.get("country", {})
    continent = airport.get("continent", {})

    cursor.execute("""
        INSERT OR IGNORE INTO airport (
            icao_code, iata_code, name, city,
            country, continent, latitude, longitude, timezone
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        airport.get("icao"),
        airport.get("iata"),
        airport.get("fullName") or airport.get("shortName"),
        airport.get("municipalityName"),
        country.get("name") if isinstance(country, dict) else country,
        continent.get("name") if isinstance(continent, dict) else continent,
        airport.get("location", {}).get("lat"),
        airport.get("location", {}).get("lon"),
        airport.get("timeZone")
    ))

    conn.commit()


# ============================================================
# FETCH FLIGHTS (FULL DAY, UTC SAFE)
# ============================================================

def fetch_flights(iata_code: str, date_str: str) -> Dict:
    """Fetch arrivals and departures for a full IST day."""
    tz_local = pytz.timezone("Asia/Kolkata")
    tz_utc = pytz.UTC

    local_start = tz_local.localize(datetime.strptime(date_str, "%Y-%m-%d"))
    local_mid = local_start + timedelta(hours=12)
    local_end = local_start + timedelta(hours=23, minutes=59)

    windows = [
        (local_start.astimezone(tz_utc), local_mid.astimezone(tz_utc)),
        ((local_mid + timedelta(minutes=1)).astimezone(tz_utc),
         local_end.astimezone(tz_utc))
    ]

    arrivals, departures = [], []
    seen = set()

    params = {
        "withLeg": "true",
        "withCancelled": "true",
        "withCodeshared": "true",
        "withCargo": "true",
        "withPrivate": "true"
    }

    for start, end in windows:
        from_ts = start.strftime("%Y-%m-%dT%H:%M")
        to_ts = end.strftime("%Y-%m-%dT%H:%M")

        url = f"https://{API_HOST}/flights/airports/iata/{iata_code}/{from_ts}/{to_ts}"
        print(f"➡️ Flights UTC: {from_ts} → {to_ts}")

        response = requests.get(url, headers=HEADERS, params=params, timeout=10)

        if response.status_code == 400:
            print("⚠️ Skipping invalid window")
            continue

        response.raise_for_status()
        data = response.json()

        for f in data.get("departures", []):
            key = f"{f.get('number')}_DEP"
            if key not in seen:
                departures.append(f)
                seen.add(key)

        for f in data.get("arrivals", []):
            key = f"{f.get('number')}_ARR"
            if key not in seen:
                arrivals.append(f)
                seen.add(key)

        time.sleep(1.5)

    return {"departures": departures, "arrivals": arrivals}


# ============================================================
# FLIGHTS → DATAFRAME
# ============================================================

def flights_to_dataframe(flights: Dict, airport_iata: str) -> pd.DataFrame:
    """Normalize flight JSON to DataFrame."""
    rows = []

    for flight in flights["departures"]:
        AIRCRAFT_REGISTRATIONS.add(flight.get("aircraft", {}).get("reg"))
        rows.append({
            "flight_id": f"{flight.get('number')}_{flight.get('departure', {}).get('scheduledTime', {}).get('utc')}",
            "flight_number": flight.get("number"),
            "aircraft_registration": flight.get("aircraft", {}).get("reg"),
            "origin_iata": airport_iata,
            "destination_iata": flight.get("arrival", {}).get("airport", {}).get("iata"),
            "scheduled_departure": flight.get("departure", {}).get("scheduledTime", {}).get("utc"),
            "actual_departure": flight.get("departure", {}).get("revisedTime", {}).get("utc"),
            "scheduled_arrival": flight.get("arrival", {}).get("scheduledTime", {}).get("utc"),
            "actual_arrival": flight.get("arrival", {}).get("revisedTime", {}).get("utc"),
            "status": flight.get("status"),
            "airline_code": flight.get("airline", {}).get("iata")
        })

    for flight in flights["arrivals"]:
        AIRCRAFT_REGISTRATIONS.add(flight.get("aircraft", {}).get("reg"))
        rows.append({
            "flight_id": f"{flight.get('number')}_{flight.get('arrival', {}).get('scheduledTime', {}).get('utc')}",
            "flight_number": flight.get("number"),
            "aircraft_registration": flight.get("aircraft", {}).get("reg"),
            "origin_iata": flight.get("departure", {}).get("airport", {}).get("iata"),
            "destination_iata": airport_iata,
            "scheduled_departure": flight.get("departure", {}).get("scheduledTime", {}).get("utc"),
            "actual_departure": flight.get("departure", {}).get("revisedTime", {}).get("utc"),
            "scheduled_arrival": flight.get("arrival", {}).get("scheduledTime", {}).get("utc"),
            "actual_arrival": flight.get("arrival", {}).get("revisedTime", {}).get("utc"),
            "status": flight.get("status"),
            "airline_code": flight.get("airline", {}).get("iata")
        })

    return pd.DataFrame(rows).drop_duplicates(subset=["flight_id"])


# ============================================================
# AIRPORT DELAY METRICS
# ============================================================

def compute_airport_delay_metrics(df: pd.DataFrame, iata: str, date_str: str) -> Dict:
    """Compute daily airport delay KPIs."""
    df = df.copy()

    for col in [
        "scheduled_departure",
        "actual_departure",
        "scheduled_arrival",
        "actual_arrival"
    ]:
        df[col] = pd.to_datetime(
            df[col],
            errors="coerce",
            utc=True   # 🔑 THIS IS THE FIX
        )

    dep = df.loc[
        (df["origin_iata"] == iata) &
        df["scheduled_departure"].notna() &
        df["actual_departure"].notna()
    ].copy()

    dep["delay"] = dep["actual_departure"] - dep["scheduled_departure"]
    dep["delay"] = dep["delay"].dt.total_seconds() / 60
    dep["delay"] = dep["delay"].clip(lower=0)

    arr = df.loc[
        (df["destination_iata"] == iata) &
        df["scheduled_arrival"].notna() &
        df["actual_arrival"].notna()
    ].copy()

    arr["delay"] = pd.to_timedelta(arr["actual_arrival"] - arr["scheduled_arrival"]).dt.total_seconds() / 60
    arr["delay"] = arr["delay"].clip(lower=0)

    dep["delay"] = dep["actual_arrival"] - dep["scheduled_arrival"]
    dep["delay"] = dep["delay"].dt.total_seconds() / 60
    dep["delay"] = dep["delay"].clip(lower=0)


    delays = pd.concat([dep["delay"], arr["delay"]])

    cancelled = df[
        df["status"].str.lower().isin(["cancelled", "canceled"]) &
        ((df["origin_iata"] == iata) | (df["destination_iata"] == iata))
    ].shape[0]

    return {
        "airport_iata": iata,
        "delay_date": date_str,
        "total_flights": len(dep) + len(arr),
        "delayed_flights": int((delays > 0).sum()),
        "avg_delay_min": int(delays.mean()) if not delays.empty else 0,
        "median_delay_min": int(delays.median()) if not delays.empty else 0,
        "canceled_flights": cancelled
    }


def insert_airport_delay(conn, metrics: Dict) -> None:
    """Insert airport delay metrics."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO airport_delays (
            airport_iata, delay_date, total_flights,
            delayed_flights, avg_delay_min,
            median_delay_min, canceled_flights
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        metrics["airport_iata"],
        metrics["delay_date"],
        metrics["total_flights"],
        metrics["delayed_flights"],
        metrics["avg_delay_min"],
        metrics["median_delay_min"],
        metrics["canceled_flights"]
    ))
    conn.commit()


# ============================================================
# AIRCRAFT FUNCTIONS
# ============================================================

def fetch_aircraft_data(registration: str) -> Optional[Dict]:
    """Fetch aircraft metadata."""
    url = f"https://{API_HOST}/aircrafts/reg/{registration}"
    response = requests.get(url, headers=HEADERS, timeout=10)

    if response.status_code != 200 or not response.text.strip():
        return None

    return response.json()


def insert_aircraft(conn, aircraft: Optional[Dict]) -> None:
    """
    Insert aircraft metadata into the database.

    Safely handles:
    - None payloads
    - Missing keys
    - SQL errors
    """
    if not aircraft:
        return

    try:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR IGNORE INTO aircraft (
                registration,
                model,
                manufacturer,
                icao_type_code,
                owner
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            aircraft.get("reg"),
            aircraft.get("modelCode", ""),
            aircraft.get("typeName", ""),
            aircraft.get("icaoCode",""),
            aircraft.get("airlineName", "")
        ))

        conn.commit()

    except sqlite3.Error as db_err:
        print(
            f"❌ DB error inserting aircraft {aircraft.get('reg')}: {db_err}"
        )

    except Exception as err:
        print(
            f"❌ Unexpected error inserting aircraft {aircraft.get('reg')}: {err}"
        )



# ============================================================
# ORCHESTRATOR
# ============================================================

def run_etl(date_str: str) -> None:
    """Run full ETL pipeline."""
    # for airport in AIRPORTS:
    #     print(f"\n===== {airport} =====")

    #     insert_airport(DB_CONN, fetch_airport(airport))

    #     flights = fetch_flights(airport, date_str)
    #     df = flights_to_dataframe(flights, airport)

    #     df.to_sql("flights", DB_CONN, if_exists="append", index=False)

    #     metrics = compute_airport_delay_metrics(df, airport, date_str)
    #     insert_airport_delay(DB_CONN, metrics)

    #     print("✅ Airport delay inserted:", metrics)
    #     time.sleep(2)

    for reg in AIRCRAFT_REGISTRATIONS:
        print(f"✈️ Fetching aircraft: {reg}")
        if reg != None:
            aircraft = fetch_aircraft_data(reg)
            insert_aircraft(DB_CONN, aircraft)
        time.sleep(1.5)
        print("✅ Aircraft inserted:", reg)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_etl("2024-12-14")
