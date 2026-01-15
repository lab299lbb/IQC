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

    # --- HÀM TIỆN ÍCH CHUNG ---
    def get_data(self, table_name):
        """Lấy toàn bộ dữ liệu từ một bảng"""
        try:
            response = self.supabase.table(table_name).select("*").execute()
            return pd.DataFrame(response.data)
        except Exception as e:
            print(f"Lỗi lấy dữ liệu {table_name}: {e}")
            return pd.DataFrame()
    def get_all_devices(self):
        """Lấy danh sách các thiết bị (máy xét nghiệm) duy nhất từ bảng tests"""
        try:
            # Lấy cột device từ bảng tests
            response = self.supabase.table("tests").select("device").execute()
            if response.data:
                df = pd.DataFrame(response.data)
                # Loại bỏ giá trị trống và trùng lặp, sau đó sắp xếp
                devices = df['device'].dropna().unique().tolist()
                return sorted([d for d in devices if str(d).strip() != ""])
            return []
        except Exception as e:
            print(f"Lỗi lấy danh sách thiết bị: {e}")
            return []
    # --- QUẢN LÝ TESTS ---
    def get_all_tests(self):
        return self.get_data("tests")
    def get_test_by_name(self, name):
        """
        Tìm kiếm thông tin xét nghiệm dựa trên tên (name).
        Trả về Dictionary chứa thông tin xét nghiệm hoặc None nếu không tìm thấy.
        """
        try:
            # Truy vấn bảng tests, lọc theo cột name
            response = self.supabase.table("tests").select("*").eq("name", name).execute()
            
            # Nếu tìm thấy kết quả, trả về bản ghi đầu tiên
            if response.data and len(response.data) > 0:
                return response.data[0]
            return None
        except Exception as e:
            print(f"Lỗi khi tìm xét nghiệm theo tên: {e}")
            return None
    def add_test(self, name, unit, tea, device, cvi=0, cvg=0):
        data = {
            "name": name, "unit": unit, "tea": tea, 
            "device": device, "cvi": cvi, "cvg": cvg
        }
        try:
            self.supabase.table("tests").insert(data).execute()
            return True
        except: return False

    def update_test(self, test_id, name, unit, device, tea, cvi, cvg):
        """Cập nhật thông tin xét nghiệm bao gồm cả TEa, CVi, CVg lên Supabase"""
        data = {
            "name": name,
            "unit": unit,
            "device": device,
            "tea": float(tea) if tea else 0.0,
            "cvi": float(cvi) if cvi else 0.0,
            "cvg": float(cvg) if cvg else 0.0
        }
        try:
            # Thực hiện cập nhật dòng có ID tương ứng
            self.supabase.table("tests").update(data).eq("id", test_id).execute()
            return True
        except Exception as e:
            st.error(f"Lỗi cập nhật xét nghiệm: {e}")
            return False

    # --- QUẢN LÝ LOTS ---
    def add_lot(self, test_id, lot_number, level, method, expiry_date, mean, sd):
        exp_str = expiry_date.strftime('%Y-%m-%d') if isinstance(expiry_date, (datetime, date)) else str(expiry_date)
        data = {
            "test_id": test_id, "lot_number": lot_number, "level": level,
            "method": method, "expiry_date": exp_str, "mean": mean, "sd": sd
        }
        try:
            self.supabase.table("lots").insert(data).execute()
            return True
        except: return False

    def get_lots_for_test(self, test_id):
        response = self.supabase.table("lots").select("*").eq("test_id", test_id).order("id", desc=True).execute()
        return pd.DataFrame(response.data)
    def get_lots_for_test(self, test_id):
        # Danh sách các cột bắt buộc phải có để main.py không bị lỗi
        required_columns = ['id', 'test_id', 'lot_number', 'level', 'expiry_date', 'mean', 'sd']
        try:
            if not test_id:
                return pd.DataFrame(columns=required_columns)
                
            response = self.supabase.table("lots").select("*").eq("test_id", test_id).execute()
            
            if response.data and len(response.data) > 0:
                return pd.DataFrame(response.data)
            
            # Nếu không có dữ liệu, trả về DataFrame rỗng nhưng CÓ CỘT
            return pd.DataFrame(columns=required_columns)
        except Exception as e:
            return pd.DataFrame(columns=required_columns)
    # --- QUẢN LÝ IQC RESULTS ---
    def add_iqc_data(self, lot_id, dt, level, value, note):
        try:
            if isinstance(dt, (datetime, date)):
                d_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                d_str = str(dt)
            
            data = {
                "lot_id": lot_id, "date": d_str, 
                "value": value, "level": level, "note": note
            }
            # Thêm biến result để kiểm tra
            result = self.supabase.table("iqc_results").insert(data).execute()
            return True
        except Exception as e:
            # Dòng này cực kỳ quan trọng: Nó sẽ hiện lỗi thật sự lên App
            st.error(f"Lỗi Supabase Insert: {e}")
            return False
    def get_iqc_data_by_lot(self, lot_id):
        response = self.supabase.table("iqc_results").select("*").eq("lot_id", lot_id).order("date", desc=True).execute()
        return pd.DataFrame(response.data)

    def delete_iqc_result(self, row_id):
        try:
            self.supabase.table("iqc_results").delete().eq("id", row_id).execute()
            return True
        except: return False
    def get_iqc_data_continuous(self, test_id, max_months=None):
        """
        Lấy dữ liệu IQC liên tục của một xét nghiệm qua API Supabase.
        Thực hiện Join bảng iqc_results và lots để lấy tên số Lô.
        """
        try:
            # Truy vấn iqc_results và lấy thêm thông tin lot_number từ bảng lots (Foreign Key)
            query = self.supabase.table("iqc_results") \
                .select("*, lots(lot_number, test_id)") \
                .eq("lots.test_id", test_id) \
                .order("date", desc=False)
            
            # Nếu có giới hạn thời gian (ví dụ 3 tháng gần nhất)
            if max_months:
                from datetime import datetime, timedelta
                start_date = (datetime.now() - timedelta(days=max_months * 30)).strftime('%Y-%m-%d')
                query = query.gte("date", start_date)
            
            response = query.execute()
            
            if not response.data:
                return pd.DataFrame()
                
            df = pd.DataFrame(response.data)
            
            # Xử lý dữ liệu lồng nhau từ Supabase (lots: {lot_number: '...' }) thành cột phẳng
            if 'lots' in df.columns:
                df['lot_number'] = df['lots'].apply(lambda x: x['lot_number'] if isinstance(x, dict) else None)
                # Xóa cột lots cũ để tránh nhầm lẫn
                df = df.drop(columns=['lots'])
                
            return df
        except Exception as e:
            print(f"Lỗi get_iqc_data_continuous: {e}")
            return pd.DataFrame()
    # --- QUẢN LÝ MAPPING ---
    def add_mapping(self, test_id, external_name):
        data = {"test_id": test_id, "external_name": external_name}
        try:
            # Supabase sử dụng upsert dựa trên cột đã đặt unique
            self.supabase.table("test_mapping").upsert(data).execute()
            return True
        except: return False

    def get_all_mappings(self):
        # Join bảng trong Supabase: lấy mapping kèm tên test
        response = self.supabase.table("test_mapping").select("id, external_name, test_id, tests(name)").execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df['internal_name'] = df['tests'].apply(lambda x: x['name'] if x else None)
        return df
    def delete_mapping(self, mapping_id):
        """Xóa một dòng ánh xạ (mapping) dựa trên ID"""
        try:
            self.supabase.table("name_mapping").delete().eq("id", mapping_id).execute()
            return True
        except Exception as e:
            st.error(f"Lỗi khi xóa mapping: {e}")
            return False
    # --- QUẢN LÝ EQA ---
    def add_eqa(self, data):
        try:
            # Chuyển date sang string nếu là object
            if 'date' in data and isinstance(data['date'], (datetime, date)):
                data['date'] = data['date'].strftime('%Y-%m-%d')
            self.supabase.table("eqa_results").insert(data).execute()
            return True
        except: return False

    def get_eqa_data(self, test_id):
        response = self.supabase.table("eqa_results").select("*").eq("test_id", test_id).order("date", desc=True).execute()
        return pd.DataFrame(response.data)

    # --- SETTINGS ---
    def get_setting(self, key, default=None):
        try:
            res = self.supabase.table("settings").select("value").eq("key", key).execute()
            return res.data[0]['value'] if res.data else default
        except: return default

    def set_setting(self, key, value):
        try:
            self.supabase.table("settings").upsert({"key": key, "value": str(value)}).execute()
            return True
        except: return False









