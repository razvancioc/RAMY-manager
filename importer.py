import re
import requests
from bs4 import BeautifulSoup
from typing import Dict, Any, List, Optional

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html, */*',
    'Referer': 'https://makerworld.com/',
    'Origin': 'https://makerworld.com'
}

ROMANIAN_COLOR_PALETTE = [
    ("#000000", "Negru"),
    ("#111827", "Negru"),
    ("#FFFFFF", "Alb"),
    ("#F9FAFB", "Alb"),
    ("#E5E7EB", "Alb / Gri Deschis"),
    ("#E02424", "Roșu"),
    ("#DC2626", "Roșu"),
    ("#FF0000", "Roșu"),
    ("#991B1B", "Roșu Închis / Bordeaux"),
    ("#00E575", "Verde Bambu"),
    ("#00AE42", "Verde"),
    ("#10B981", "Verde"),
    ("#059669", "Verde Smarald"),
    ("#008000", "Verde"),
    ("#1E40AF", "Albastru"),
    ("#2563EB", "Albastru"),
    ("#3B82F6", "Albastru Deschis"),
    ("#0000FF", "Albastru"),
    ("#0284C7", "Bleu"),
    ("#06B6D4", "Turcoaz"),
    ("#00FFFF", "Cyan"),
    ("#FACA15", "Galben"),
    ("#EAB308", "Galben"),
    ("#FFFF00", "Galben"),
    ("#FF7A00", "Portocaliu"),
    ("#F97316", "Portocaliu"),
    ("#FF8C00", "Portocaliu"),
    ("#7E3AF2", "Mov"),
    ("#8B5CF6", "Violet"),
    ("#9333EA", "Mov"),
    ("#800080", "Purpuriu"),
    ("#8B4513", "Maro"),
    ("#78350F", "Maro"),
    ("#A52A2A", "Maro"),
    ("#6B7280", "Gri"),
    ("#9CA3AF", "Gri Deschis"),
    ("#4B5563", "Gri"),
    ("#374151", "Gri Antracit"),
    ("#EC4899", "Roz"),
    ("#F43F5E", "Roz / Magenta"),
    ("#FFC0CB", "Roz Deschis"),
    ("#D97706", "Auriu / Chihlimbar"),
    ("#C0C0C0", "Argintiu Metalic"),
]

def hex_to_rgb(hex_str: str) -> tuple:
    if not hex_str:
        return (128, 128, 128)
    clean = hex_str.lstrip("#")
    if len(clean) >= 6:
        try:
            return int(clean[0:2], 16), int(clean[2:4], 16), int(clean[4:6], 16)
        except Exception:
            return (128, 128, 128)
    return (128, 128, 128)

def get_color_name(hex_code: str) -> str:
    if not hex_code:
        return "Colorat"
    r1, g1, b1 = hex_to_rgb(hex_code)
    best_name = "Colorat"
    best_dist = float("inf")
    for ref_hex, name in ROMANIAN_COLOR_PALETTE:
        r2, g2, b2 = hex_to_rgb(ref_hex)
        dist = ((r1 - r2) ** 2 + (g1 - g2) ** 2 + (b1 - b2) ** 2) ** 0.5
        if dist < best_dist:
            best_dist = dist
            best_name = name
    return best_name

def parse_makerworld_details(url: str) -> Optional[Dict[str, Any]]:
    mw_match = re.search(r'makerworld\.com/(?:[a-z]{2}/)?models/(\d+)', url)
    if not mw_match:
        return None
    
    design_id = mw_match.group(1)
    profile_match = re.search(r'#profileId-(\d+)', url) or re.search(r'[?&]profileId=(\d+)', url)
    target_profile_id = int(profile_match.group(1)) if profile_match else None

    try:
        api_url = f"https://makerworld.com/api/v1/design-service/design/{design_id}"
        resp = requests.get(api_url, headers=DEFAULT_HEADERS, timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None

        title = (data.get("titleTranslated") or data.get("title") or "Model MakerWorld").strip()
        cover = data.get("coverUrl") or ""
        creator = data.get("designCreator") or {}
        author_name = creator.get("name") or "MakerWorld Creator"
        author_avatar = creator.get("avatar") or ""
        summary = (data.get("summaryTranslated") or data.get("summary") or "").strip()
        
        # Look for instances (print profiles)
        instances = data.get("instances")
        if not instances or not isinstance(instances, list):
            instances = data.get("designExtension", {}).get("profileExtension", {}).get("profiles") or []
        
        variante: List[Dict[str, Any]] = []

        for idx, inst in enumerate(instances):
            if not isinstance(inst, dict):
                continue
            
            inst_id = inst.get("id") or inst.get("instanceId") or (idx + 1)
            inst_title = (inst.get("titleTranslated") or inst.get("title") or f"Profil #{inst_id}").strip()
            inst_summary = (inst.get("summaryTranslated") or inst.get("summary") or "").strip()
            
            pred_time_raw = inst.get("prediction", 0) or inst.get("predictTime", 0)
            if pred_time_raw > 1000:
                time_min = round(pred_time_raw / 60)
            else:
                time_min = int(pred_time_raw or 0)
                
            weight = float(inst.get("weight", 0.0) or 0.0)
            is_default = bool(inst.get("isDefault", False))
            
            # Selection logic
            selected = False
            if target_profile_id and inst_id == target_profile_id:
                selected = True
            elif not target_profile_id and (is_default or idx == 0):
                selected = True
                
            model_info = inst.get("extention", {}).get("modelInfo", {}) if isinstance(inst.get("extention"), dict) else {}
            compat = model_info.get("compatibility") or {}
            dev_name = compat.get("devProductName") or inst.get("printerModel") or "Bambu A1 / P1 / X1"
            nozzle = compat.get("nozzleDiameter") or inst.get("nozzleDiameter") or 0.4
            
            other_compat_raw = model_info.get("otherCompatibility") or []
            other_compat = [c.get("devProductName") for c in other_compat_raw if isinstance(c, dict) and c.get("devProductName")]
            
            # Profile filaments
            raw_filaments = inst.get("instanceFilaments") or []
            filamente_profil = []
            for f in raw_filaments:
                if not isinstance(f, dict):
                    continue
                c_hex = f.get("color") or "#FFFFFF"
                filamente_profil.append({
                    "type": f.get("type", "PLA"),
                    "color": c_hex,
                    "color_name": get_color_name(c_hex),
                    "used_g": float(f.get("usedG", 0) or 0),
                    "used_m": float(f.get("usedM", 0) or 0)
                })

            # Build Plates
            raw_plates = model_info.get("plates") or []
            if not raw_plates:
                raw_plates = inst.get("lacInfo", {}).get("plates") or []

            plateuri = []
            for p_idx, pl in enumerate(raw_plates):
                if not isinstance(pl, dict):
                    continue
                actual_idx = pl.get("index", p_idx + 1)
                pl_name = pl.get("name") or f"Placa {actual_idx}"
                
                thumb_obj = pl.get("thumbnail") or pl.get("top_picture") or pl.get("pick_picture") or {}
                thumb_url = thumb_obj.get("url") if isinstance(thumb_obj, dict) else ""
                if not thumb_url:
                    thumb_url = cover
                    
                pl_time_raw = pl.get("prediction", 0) or pl.get("predictTime", 0)
                if pl_time_raw > 1000:
                    pl_time_min = round(pl_time_raw / 60)
                else:
                    pl_time_min = int(pl_time_raw or 0)
                pl_weight = float(pl.get("weight", 0.0) or 0.0)
                
                pl_filaments = []
                for pf in pl.get("filaments", []):
                    if not isinstance(pf, dict):
                        continue
                    pf_hex = pf.get("color") or "#FFFFFF"
                    pl_filaments.append({
                        "id": pf.get("id"),
                        "type": pf.get("type", "PLA"),
                        "color": pf_hex,
                        "color_name": get_color_name(pf_hex),
                        "used_g": float(pf.get("usedG", 0) or 0),
                        "used_m": float(pf.get("usedM", 0) or 0)
                    })
                    
                plateuri.append({
                    "index": actual_idx,
                    "nume": pl_name,
                    "thumbnail_url": thumb_url,
                    "timp_minute": pl_time_min,
                    "greutate_g": pl_weight,
                    "filamente": pl_filaments
                })
                
            # If no plates defined in metadata, create 1 single plate with profile values
            if not plateuri:
                plateuri.append({
                    "index": 1,
                    "nume": "Placa 1 (Completă)",
                    "thumbnail_url": cover,
                    "timp_minute": time_min,
                    "greutate_g": weight,
                    "filamente": filamente_profil
                })

            variante.append({
                "id": inst_id,
                "titlu": inst_title,
                "summary": inst_summary,
                "timp_minute": time_min,
                "greutate_g": weight,
                "imprimanta": dev_name,
                "duza": nozzle,
                "alte_compatibilitati": other_compat,
                "is_default": is_default,
                "selected": selected,
                "descarcari": inst.get("downloadCount", 0),
                "printuri": inst.get("printCount", 0),
                "rating": round(float(inst.get("score", 0.0) or 0.0), 1),
                "filamente": filamente_profil,
                "nr_plateuri": len(plateuri),
                "plateuri": plateuri
            })

        # Ensure at least one variant is selected
        if variante and not any(v.get("selected") for v in variante):
            variante[0]["selected"] = True

        first_var = next((v for v in variante if v.get("selected")), (variante[0] if variante else None))
        total_time = first_var["timp_minute"] if first_var else 0
        total_weight = first_var["greutate_g"] if first_var else 0.0

        return {
            "success": True,
            "sursa": "MakerWorld",
            "design_id": design_id,
            "nume_proiect": title,
            "link_imagine": cover,
            "link_proiect": url,
            "autor": author_name,
            "autor_avatar": author_avatar,
            "descriere": summary,
            "timp_estimat_minute": total_time,
            "greutate_estimata": total_weight,
            "nr_variante": len(variante),
            "variante": variante
        }
    except Exception as e:
        print(f"Error parsing MakerWorld details: {e}")
        return None

def import_model_from_url(url: str) -> Dict[str, Any]:
    url = url.strip()
    result = {
        "success": False,
        "nume_proiect": "",
        "link_imagine": "",
        "link_proiect": url,
        "timp_estimat_minute": 0,
        "greutate_estimata": 0.0,
        "descriere": "",
        "sursa": "Necunoscut",
        "autor": "",
        "autor_avatar": "",
        "nr_variante": 0,
        "variante": []
    }
    
    if not url:
        return result

    # 1. Try MakerWorld API
    if "makerworld.com" in url:
        mw_data = parse_makerworld_details(url)
        if mw_data:
            return mw_data

    # 2. Generic OpenGraph Scraping
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=6)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            og_title = soup.find('meta', property='og:title') or soup.find('title')
            og_image = soup.find('meta', property='og:image')
            og_desc = soup.find('meta', property='og:description')
            
            title = ""
            if og_title:
                title = og_title.get('content', '') if og_title.name == 'meta' else og_title.text
            image = og_image.get('content', '') if og_image else ''
            desc = og_desc.get('content', '') if og_desc else ''
            
            title = title.strip()[:120]
            image = image.strip()
            desc = desc.strip()[:240]

            default_variant = {
                "id": 1,
                "titlu": "Profil Standard",
                "summary": desc,
                "timp_minute": 0,
                "greutate_g": 0.0,
                "imprimanta": "Universal",
                "duza": 0.4,
                "alte_compatibilitati": [],
                "is_default": True,
                "selected": True,
                "descarcari": 0,
                "printuri": 0,
                "rating": 0.0,
                "filamente": [],
                "nr_plateuri": 1,
                "plateuri": [
                    {
                        "index": 1,
                        "nume": "Placa 1",
                        "thumbnail_url": image,
                        "timp_minute": 0,
                        "greutate_g": 0.0,
                        "filamente": []
                    }
                ]
            }

            result.update({
                "success": bool(title or image),
                "nume_proiect": title,
                "link_imagine": image,
                "descriere": desc,
                "sursa": "Web",
                "nr_variante": 1,
                "variante": [default_variant]
            })
    except Exception as e:
        print(f"Error scraping generic URL: {e}")

    return result
