import pandas as pd
import streamlit as st
from supabase import create_client

def import_lots_from_excel(file):
    # 1. Kết nối Supabase (lấy từ secrets)
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase = create_client(url, key)

    try:
        # 2. Đọc file Excel
        df = pd.read_excel(file)
        
        # 3. Lấy danh sách Tests hiện có để ánh xạ (Map) tên -> ID
        tests_res = supabase.table("tests").select("id, name").execute()
        test_map = {t['name']: t['id'] for t in tests_res.data}

        success_count = 0
        error_list = []

        # 4. Duyệt từng dòng trong Excel để đẩy lên
        for index, row in df.iterrows():
            test_name = str(row['test_name']).strip()
            
            # Kiểm tra xem tên xét nghiệm có tồn tại trong DB không
            if test_name in test_map:
                data = {
                    "test_id": int(test_map[test_name]), # Lấy ID tương ứng
                    "lot_number": str(row['lot_number']),
                    "level": int(row['level']),
                    "mean": float(row['mean']),
                    "sd": float(row['sd']),
                    "expiry_date": str(row['expiry_date']),
                    "method": str(row.get('method', 'Imported'))
                }
                
                # Đẩy lên Supabase
                res = supabase.table("lots").insert(data).execute()
                if res.data:
                    success_count += 1
            else:
                error_list.append(f"Dòng {index+2}: Không tìm thấy xét nghiệm '{test_name}'")

        return success_count, error_list

    except Exception as e:
        st.error(f"Lỗi hệ thống: {e}")
        return 0, [str(e)]

# --- Giao diện Streamlit ---
st.title("Import Lot từ Excel")
uploaded_file = st.file_uploader("Chọn file Excel (.xlsx)", type=["xlsx"])

if uploaded_file and st.button("Bắt đầu Import"):
    with st.spinner("Đang xử lý dữ liệu..."):
        success, errors = import_lots_from_excel(uploaded_file)
        
    st.success(f"Đã import thành công {success} dòng!")
    if errors:
        with st.expander("Xem các dòng lỗi"):
            for err in errors:
                st.warning(err)import pandas as pd
import streamlit as st
from supabase import create_client

def import_lots_from_excel(file):
    # 1. Kết nối Supabase (lấy từ secrets)
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase = create_client(url, key)

    try:
        # 2. Đọc file Excel
        df = pd.read_excel(file)
        
        # 3. Lấy danh sách Tests hiện có để ánh xạ (Map) tên -> ID
        tests_res = supabase.table("tests").select("id, name").execute()
        test_map = {t['name']: t['id'] for t in tests_res.data}

        success_count = 0
        error_list = []

        # 4. Duyệt từng dòng trong Excel để đẩy lên
        for index, row in df.iterrows():
            test_name = str(row['test_name']).strip()
            
            # Kiểm tra xem tên xét nghiệm có tồn tại trong DB không
            if test_name in test_map:
                data = {
                    "test_id": int(test_map[test_name]), # Lấy ID tương ứng
                    "lot_number": str(row['lot_number']),
                    "level": int(row['level']),
                    "mean": float(row['mean']),
                    "sd": float(row['sd']),
                    "expiry_date": str(row['expiry_date']),
                    "method": str(row.get('method', 'Imported'))
                }
                
                # Đẩy lên Supabase
                res = supabase.table("lots").insert(data).execute()
                if res.data:
                    success_count += 1
            else:
                error_list.append(f"Dòng {index+2}: Không tìm thấy xét nghiệm '{test_name}'")

        return success_count, error_list

    except Exception as e:
        st.error(f"Lỗi hệ thống: {e}")
        return 0, [str(e)]

# --- Giao diện Streamlit ---
st.title("Import Lot từ Excel")
uploaded_file = st.file_uploader("Chọn file Excel (.xlsx)", type=["xlsx"])

if uploaded_file and st.button("Bắt đầu Import"):
    with st.spinner("Đang xử lý dữ liệu..."):
        success, errors = import_lots_from_excel(uploaded_file)
        
    st.success(f"Đã import thành công {success} dòng!")
    if errors:
        with st.expander("Xem các dòng lỗi"):
            for err in errors:
                st.warning(err)