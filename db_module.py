# File: db_module.py
import streamlit as st
from supabase import create_client
import pandas as pd
from datetime import datetime, date
import numpy as np

class DBManager:
    def __init__(self):
        # Kết nối qua HTTP API (Lấy từ Streamlit Secrets)
        try:
            self.url = st.secrets["supabase"]["url"]
            self.key = st.secrets["supabase"]["key"]
            self.supabase = create_client(self.url, self.key)
        except Exception as e:
            st.error(f"Lỗi cấu hình Secrets: {e}")
    def get_setting(self, key, default=None):
        try:
            response = self.supabase.table("settings").select("value").eq("key", key).execute()
            if response.data:
                return response.data[0]['value']
            return default
        except:
            return default

    def create_tables(self):
        """
        Lưu ý: Với Supabase, bạn nên chạy SQL Script trong Dashboard.
        Hàm này giữ lại logic để đảm bảo tính nhất quán của code cũ.
        """
        pass

    def upgrade_tables(self):
        """Tự động thêm các cột còn thiếu - Giữ nguyên logic cũ"""
        # Trong Supabase, việc upgrade thường làm qua SQL Editor dashboard.
        # Để giữ code không lỗi, chúng ta để pass hoặc thực hiện check column qua API.
        pass

    def update_test(self, test_id, name, unit, device, tea, cvi, cvg):
        """Cập nhật thông tin xét nghiệm bao gồm cả TEa, CVi, CVg"""
        try:
            data = {
                "name": name,
                "unit": unit,
                "device": device,
                "tea": tea,
                "cvi": cvi,
                "cvg": cvg
            }
            self.supabase.table("tests").update(data).eq("id", test_id).execute()
            return True
        except Exception as e:
            print(f"Lỗi cập nhật Database: {e}")
            return False

    def delete_test(self, test_id):
        """Xóa Test VÀ TẤT CẢ dữ liệu liên quan."""
        try:
            # Lấy danh sách lot_id liên quan
            lots_res = self.supabase.table("lots").select("id").eq("test_id", test_id).execute()
            lot_ids = [row['id'] for row in lots_res.data]
            
            if lot_ids:
                self.supabase.table("iqc_results").delete().in_("lot_id", lot_ids).execute()
                # Xóa iqc_data nếu bảng này tồn tại riêng biệt trong logic cũ
                try: self.supabase.table("iqc_data").delete().in_("lot_id", lot_ids).execute()
                except: pass
            
            self.supabase.table("lots").delete().eq("test_id", test_id).execute()
            self.supabase.table("eqa_results").delete().eq("test_id", test_id).execute()
            # Xóa eqa_data nếu bảng này tồn tại riêng biệt trong logic cũ
            try: self.supabase.table("eqa_data").delete().eq("test_id", test_id).execute()
            except: pass
            
            self.supabase.table("tests").delete().eq("id", test_id).execute()
            return True
        except Exception as e:
            print(f"LỖI DB: Không thể xóa Test ID {test_id}: {e}")
            return False

    # --- QUẢN LÝ THIẾT BỊ & TESTS ---
    def get_all_devices(self):
        try:
            res = self.supabase.table("tests").select("device").not_.is_("device", "null").execute()
            devices = list(set([row['device'] for row in res.data if row['device'] != '']))
            devices.sort()
            return devices
        except: return []

    def add_test(self, name, unit, tea, device, cvi=0, cvg=0):
        try:
            data = {"name": name, "unit": unit, "tea": tea, "device": device, "cvi": cvi, "cvg": cvg}
            self.supabase.table("tests").insert(data).execute()
            return True
        except: return False

    def get_all_tests(self):
        try:
            # Lấy tất cả các cột để đảm bảo có 'unit', 'device', 'method'...
            res = self.supabase.table("tests").select("*").execute() 
            return pd.DataFrame(res.data)
        except Exception as e:
            st.error(f"Lỗi truy vấn danh sách xét nghiệm: {e}")
            return pd.DataFrame()
        
    def update_test_info(self, test_id, name, unit, tea, device, cvi, cvg):
        try:
            data = {"name": name, "unit": unit, "tea": tea, "device": device, "cvi": cvi, "cvg": cvg}
            self.supabase.table("tests").update(data).eq("id", test_id).execute()
            return True
        except: return False

    # --- QUẢN LÝ LOTS ---
    def add_lot(self, test_id, lot_number, level, method, expiry_date, mean, sd):
        try:
            exp_str = expiry_date.strftime('%Y-%m-%d') if isinstance(expiry_date, (datetime, pd.Timestamp, date)) else str(expiry_date)
            data = {
                "test_id": test_id, "lot_number": lot_number, "level": level, 
                "method": method, "expiry_date": exp_str, "mean": mean, "sd": sd
            }
            self.supabase.table("lots").insert(data).execute()
            return True
        except: return False

    def get_lots_for_test(self, test_id):
        # Lấy danh sách xét nghiệm
        df_tests = db.get_all_tests()
        # Tạo selectbox hiển thị tên nhưng lưu giá trị là ID
        selected_test_name = st.selectbox("Chọn xét nghiệm", df_tests['name'].unique())
        selected_test_id = df_tests[df_tests['name'] == selected_test_name]['id'].values[0]
        
        # Dùng ID này để lấy Lot
        df_lots = db.get_lots_for_test(selected_test_id)
        
        if not df_lots.empty:
            selected_lot = st.selectbox("Chọn Lot", df_lots['lot_number'].unique())
        else:
            st.warning("Xét nghiệm này chưa có dữ liệu Lot!")
        
        # Dòng debug này giúp bạn kiểm tra xem Supabase có trả về dữ liệu hay không
        print(f"DEBUG: Đang tìm Lot cho Test ID: {test_id}, Kết quả: {res.data}")
        
        return pd.DataFrame(res.data)

    def update_lot_params(self, lot_id, lot_number, method, expiry_date, mean, sd):
        try:
            exp_str = expiry_date.strftime('%Y-%m-%d') if isinstance(expiry_date, (datetime, pd.Timestamp, date)) else str(expiry_date)
            data = {"lot_number": lot_number, "method": method, "expiry_date": exp_str, "mean": mean, "sd": sd}
            self.supabase.table("lots").update(data).eq("id", lot_id).execute()
            return True
        except: return False
            
    def delete_lot(self, lot_id):
        try:
            self.supabase.table("lots").delete().eq("id", lot_id).execute()
            return True
        except Exception as e:
            print(f"Lỗi khi xóa Lot: {e}")
            return False

    def update_lot(self, lot_id, lot_number, mean, sd, expiration_date):
        try:
            data = {"lot_number": lot_number, "mean": mean, "sd": sd, "expiry_date": expiration_date}
            self.supabase.table("lots").update(data).eq("id", lot_id).execute()
            return True
        except Exception as e:
            print(f"Lỗi khi cập nhật Lot: {e}")
            return False

    def get_test_by_name(self, name):
        res = self.supabase.table("tests").select("id").eq("name", name).execute()
        return {'id': res.data[0]['id']} if res.data else None

    # --- QUẢN LÝ IQC DATA ---
    def add_iqc_data(self, lot_id, dt, level, value, note):
        try:
            if isinstance(dt, str):
                dt_obj = pd.to_datetime(dt, dayfirst=True)
                d_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                d_str = dt.strftime('%Y-%m-%d %H:%M:%S')

            data = {"lot_id": lot_id, "date": d_str, "value": value, "level": level, "note": note}
            self.supabase.table("iqc_results").insert(data).execute()
            return True
        except Exception as e:
            print(f"Lỗi chuẩn hóa ngày: {e}")
            return False
                   
    def update_iqc_action(self, result_id, action_text):
        self.supabase.table("iqc_results").update({"action": action_text}).eq("id", result_id).execute()

    def get_iqc_data_continuous(self, test_id, max_months=None):
        # Supabase thực hiện join qua cú pháp select
        query = self.supabase.table("iqc_results").select("*, lots!inner(lot_number, test_id)")
        query = query.eq("lots.test_id", test_id)
        
        if max_months:
            cutoff = (datetime.now() - timedelta(days=max_months*30)).strftime('%Y-%m-%d')
            query = query.gte("date", cutoff)
            
        res = query.order("date", desc=False).execute()
        # Flatten dữ liệu join
        flat_data = []
        for r in res.data:
            r['lot_number'] = r['lots']['lot_number']
            flat_data.append(r)
        return pd.DataFrame(flat_data)

    def get_iqc_data_filtered(self, test_id, d_start, d_end):
        s_date = d_start.strftime('%Y-%m-%d')
        e_date = d_end.strftime('%Y-%m-%d')
        res = self.supabase.table("iqc_results").select("*, lots!inner(test_id)")\
            .eq("lots.test_id", test_id)\
            .gte("date", s_date)\
            .lte("date", e_date)\
            .order("date", desc=False).execute()
        return pd.DataFrame(res.data)

    def get_iqc_data_by_lot(self, lot_id):
        try:
            res = self.supabase.table("iqc_results").select("id, date, value, level, note")\
                .eq("lot_id", lot_id).order("date", desc=True).execute()
            return pd.DataFrame(res.data)
        except Exception as e:
            print(f"Lỗi truy vấn: {e}")
            return pd.DataFrame()

    def get_iqc_data_by_lot_full(self, lot_id):
        res = self.supabase.table("iqc_results").select("id, date, value, note")\
            .eq("lot_id", lot_id).order("date", desc=True).execute()
        return pd.DataFrame(res.data)
        
    def update_iqc_data(self, iqc_id, note, dt, level, value):
        try:
            if isinstance(dt, (pd.Timestamp, datetime)):
                d_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                d_str = str(dt)
            data = {"date": d_str, "level": level, "value": value, "note": note}
            self.supabase.table("iqc_results").update(data).eq("id", iqc_id).execute()
            return True
        except Exception as e:
            print(f"Lỗi SQL: {e}")
            return False
  
    def delete_iqc_result(self, row_id):
        try:
            res = self.supabase.table("iqc_results").delete().eq("id", int(row_id)).execute()
            return True
        except Exception as e:
            print(f"Lỗi xóa IQC: {e}")
            return False

    def upgrade_db(self):
        # Giữ nguyên để không lỗi app, nhưng Supabase quản lý cột qua Dashboard
        pass

    def import_iqc_from_dataframe(self, df):
        success_count = 0
        errors = []
        for _, row in df.iterrows():
            try:
                ext_name = str(row['Tên xét nghiệm']).strip()
                lot_num = str(row['Lô']).strip()
                level = int(row['Mức QC'])
                value = float(row['Kết quả'])
                run_date_raw = pd.to_datetime(row['Thời gian chạy'])
                run_date_iso = run_date_raw.strftime('%Y-%m-%d %H:%M:%S')

                # Mapping
                map_res = self.supabase.table("test_mapping").select("test_id").eq("external_name", ext_name).execute()
                if not map_res.data:
                    errors.append(f"Chưa mapping tên: {ext_name}")
                    continue
                test_id = map_res.data[0]['test_id']

                # Lot lookup (Sử dụng ilike cho Supabase)
                lot_res = self.supabase.table("lots").select("id")\
                    .eq("test_id", test_id).ilike("lot_number", f"%{lot_num}%").eq("level", level).execute()

                if not lot_res.data:
                    errors.append(f"Không tìm thấy Lô {lot_num} (Mức {level}) cho XN này.")
                    continue
                lot_id = lot_res.data[0]['id']

                # Check duplicate
                dup_check = self.supabase.table("iqc_results").select("id")\
                    .eq("lot_id", lot_id).eq("date", run_date_iso).eq("value", value).execute()
                if dup_check.data: continue 

                # Insert
                ins_data = {
                    "lot_id": lot_id, "date": run_date_iso, "value": value, 
                    "level": level, "note": f"Import từ máy {row.get('Máy xét nghiệm', 'Excel')}"
                }
                self.supabase.table("iqc_results").insert(ins_data).execute()
                success_count += 1
            except Exception as e:
                errors.append(f"Lỗi dòng {row.get('Tên xét nghiệm', 'N/A')}: {str(e)}")
        return success_count, errors

    def get_iqc_results_all_sources(self, test_id):
        res = self.supabase.table("iqc_results").select("*, lots!inner(lot_number, test_id)")\
            .eq("lots.test_id", test_id).order("date", desc=True).execute()
        flat_data = []
        for r in res.data:
            r['lot_number'] = r['lots']['lot_number']
            flat_data.append(r)
        return pd.DataFrame(flat_data)

    def debug_all_iqc_data(self):
        # Supabase thực hiện left join
        res = self.supabase.table("iqc_results").select("*, lots(lot_number, tests(name))").order("date", desc=True).execute()
        flat_data = []
        for r in res.data:
            lot_info = r.get('lots', {})
            test_info = lot_info.get('tests', {}) if lot_info else {}
            flat_data.append({
                "id": r['id'],
                "lot_number": lot_info.get('lot_number') if lot_info else None,
                "test_name": test_info.get('name') if test_info else None,
                "date": r['date'],
                "value": r['value'],
                "level": r['level'],
                "note": r['note']
            })
        return pd.DataFrame(flat_data)

    def add_mapping(self, test_id, external_name):
        data = {"test_id": test_id, "external_name": external_name}
        # upsert trong supabase yêu cầu cột external_name phải có ràng buộc UNIQUE
        self.supabase.table("test_mapping").upsert(data, on_conflict="external_name").execute()

    def get_all_mappings(self):
        res = self.supabase.table("test_mapping").select("*, tests(name)").execute()
        flat_data = []
        for r in res.data:
            r['internal_name'] = r['tests']['name']
            flat_data.append(r)
        return pd.DataFrame(flat_data)

    def update_mapping(self, mapping_id, new_external_name):
        self.supabase.table("test_mapping").update({"external_name": new_external_name}).eq("id", mapping_id).execute()

    def delete_mapping(self, mapping_id):
        self.supabase.table("test_mapping").delete().eq("id", mapping_id).execute()

    def get_unmapped_tests(self, excel_test_names):
        res = self.supabase.table("test_mapping").select("external_name").execute()
        mapped_names = [row['external_name'] for row in res.data]
        unmapped = [name for name in excel_test_names if name not in mapped_names]
        return list(set(unmapped))
    
    # --- QUẢN LÝ EQA DATA ---
    def add_eqa(self, data):
        try:
            self.supabase.table("eqa_results").insert(data).execute()
            return True
        except Exception as e:
            print(f"Error adding EQA: {e}")
            return False

    def get_eqa_data(self, test_id):
        res = self.supabase.table("eqa_results").select("*").eq("test_id", test_id).order("date", desc=False).execute()
        return pd.DataFrame(res.data)
        
    def delete_eqa(self, eqa_id):
        try:
            self.supabase.table("eqa_results").delete().eq("id", int(eqa_id)).execute()
            return True
        except Exception as e:
            print(f"Lỗi SQL: {e}")
            return False

    def update_eqa(self, eqa_id, data):
        if not data: return False
        try:
            self.supabase.table("eqa_results").update(data).eq("id", eqa_id).execute()
            return True
        except Exception as e:
            print(f"Lỗi cập nhật DB: {e}")
            return False
   
    def import_eqa_from_dataframe(self, df):
        success_count = 0
        errors = []
        df.columns = [str(c).strip() for c in df.columns]
        cols = df.columns
        lab_col = next((c for c in cols if any(k in c.lower() for k in ['phòng xét nghiệm', 'kết quả', 'lab', 'pxn'])), None)
        ref_col = next((c for c in cols if any(k in c.lower() for k in ['mục tiêu', 'tham chiếu', 'target', 'ref'])), None)
        sd_col = next((c for c in cols if 'sd' in c.lower() or 'độ lệch' in c.lower()), None)
        name_col = next((c for c in cols if 'tên' in c.lower() and 'nghiệm' in c.lower()), None)
        prog_col = next((c for c in cols if any(k in c.lower() for k in ['chương trình', 'mã', 'đợt', 'program'])), None)
        date_col = next((c for c in cols if 'ngày' in c.lower()), None)

        for index, row in df.iterrows():
            try:
                if not name_col or not lab_col or not ref_col:
                    errors.append(f"Dòng {index+2}: Thiếu cột.")
                    continue
                ext_name = str(row[name_col]).strip()
                lab_val = pd.to_numeric(row[lab_col], errors='coerce')
                ref_val = pd.to_numeric(row[ref_col], errors='coerce')
                sd_group = pd.to_numeric(row[sd_col], errors='coerce') if sd_col else 0
                if pd.isna(sd_group): sd_group = 0
                program_name = str(row[prog_col]) if prog_col and not pd.isna(row[prog_col]) else "EQA Import"
                sdi_val = (lab_val - ref_val) / sd_group if sd_group > 0 else 0.0

                map_res = self.supabase.table("test_mapping").select("test_id").eq("external_name", ext_name).execute()
                if not map_res.data:
                    errors.append(f"Dòng {index+2}: Chưa map tên '{ext_name}'")
                    continue
                test_id = map_res.data[0]['test_id']
                res_date = pd.to_datetime(row[date_col]).strftime('%Y-%m-%d') if date_col and not pd.isna(row[date_col]) else datetime.now().strftime('%Y-%m-%d')

                ins_data = {
                    "test_id": test_id, "date": res_date, "lab_value": float(lab_val), 
                    "ref_value": float(ref_val), "sd_group": float(sd_group), 
                    "sdi": float(sdi_val), "program_name": program_name
                }
                self.supabase.table("eqa_results").insert(ins_data).execute()
                success_count += 1
            except Exception as e:
                errors.append(f"Lỗi dòng {index+2}: {str(e)}")
        return success_count, errors

    def upgrade_eqa_table(self):
        pass

    def calculate_rms_bias(self, df_eqa):
        """Tính toán RMS Bias từ lịch sử EQA (ISO/TS 20914)"""
        if df_eqa is None or df_eqa.empty or len(df_eqa) < 2:
            return 0.0
        biases = ((df_eqa['lab_value'] - df_eqa['ref_value']) / df_eqa['ref_value']) * 100
        rms_bias_pct = np.sqrt(np.mean(biases**2))
        return rms_bias_pct

    def get_mu_target_value(self, standard, test_data, sub_type=None):
        """Trả về mục tiêu MAU (%) dựa trên tiêu chuẩn lựa chọn"""
        if standard == "BV (Biological Variation)":
            cvi = float(test_data.get('cvi', 0.0))
            cvg = float(test_data.get('cvg', 0.0))
            if cvi == 0: return float(test_data.get('tea', 10.0))
            if sub_type == "Tối ưu": return 0.25 * cvi + 1.65 * (0.125 * np.sqrt(cvi**2 + cvg**2))
            if sub_type == "Tối thiểu": return 0.75 * cvi + 1.65 * (0.375 * np.sqrt(cvi**2 + cvg**2))
            return 0.5 * cvi + 1.65 * (0.25 * np.sqrt(cvi**2 + cvg**2))
        elif standard == "CLIA": return float(test_data.get('clia_limit', 10.0))
        elif standard == "RCPA": return float(test_data.get('rcpa_limit', 8.0))
        return float(test_data.get('tea', 10.0))

    def update_mu_review(self, test_id, review_date):
        try:
            self.supabase.table("tests").update({"last_mu_review": review_date}).eq("id", test_id).execute()
            return True
        except Exception as e:
            print(f"Error updating MU review: {e}")
            return False

    def set_setting(self, key, value):
        try:
            self.supabase.table("settings").upsert({"key": key, "value": value}, on_conflict="key").execute()
            return True
        except Exception as e:
            print(f"LỖI DB: Không thể lưu cài đặt {key}: {e}")
            return False    

    def execute_raw(self, sql):
        # Lưu ý: API Supabase không cho phép chạy SQL thô trực tiếp từ Client để bảo mật.
        # Bạn nên sử dụng các hàm API có sẵn. Hàm này để lại để tránh lỗi code cũ.
        return False

    def upgrade_database_for_pro_features(self):
        pass















