import xarray as xr
from pathlib import Path
import zipfile
import tempfile
import shutil

def extract(filepath: str) -> str:
    """
    Extract and filter ICON-EU data to Trento bounding box.
    
    Args:
        filepath: Path to downloaded .zarr.zip file
    
    Returns:
        Path to the converted .nc file
    """
    # Bounding box for Trento region
    lat_min, lat_max = 45.7, 46.4
    lon_min, lon_max = 10.5, 11.9
    
    filename = Path(filepath).name[:-4]
    # Unzip zarr archive to temp directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            
        zarr_dir = Path(temp_dir)
        # Open zarr dataset
        ds = xr.open_zarr(zarr_dir, decode_timedelta=False)
        
        # Handle latitude order (ascending or descending)
        if ds.latitude.values[0] < ds.latitude.values[-1]:
            ds_bbox = ds.sel(
                latitude=slice(lat_min, lat_max),
                longitude=slice(lon_min, lon_max)
            )
        else:
            ds_bbox = ds.sel(
                latitude=slice(lat_max, lat_min),
                longitude=slice(lon_min, lon_max)
            )
        
        # Check if subset is empty
        if ds_bbox.sizes.get("latitude", 0) == 0 or ds_bbox.sizes.get("longitude", 0) == 0:
            raise ValueError(f"Empty subset for {filepath}")
        
        # Load into memory to avoid lazy writing issues
        ds_bbox = ds_bbox.load()
        
        # Build output path (same directory as input, .nc extension)
        input_path = Path(filepath)
        output_path = input_path.parent / input_path.name.replace(".zarr.zip", ".nc")
        
        # Save as NetCDF
        ds_bbox.to_netcdf(output_path)
        
        return str(output_path)
        
    finally:
        # Cleanup temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)