"""
title: Export PDF Tool
author: Assistant & User
version: 1.3.0
requirements: reportlab>=3.0.0
license: MIT
description: Exporta texto/JSON para PDF usando ReportLab com link clicável no OpenWebUI (fallback para .txt se não disponível). Gera data URI para embed direto no chat.
"""

import os, hashlib, shutil, base64
from datetime import datetime
from typing import Optional, Callable
from pydantic import BaseModel, Field


class Tools:
    class Valves(BaseModel):
        pdf_output_dir: str = Field(
            default="/home/massarente/openwebui_exports/pdfs", 
            description="Diretório de saída (SANDBOX montado via volume - já testado e funcionando)"
        )
        
        pdf_base_url: str = Field(
            default="/sandbox/pdfs",
            description="URL base para servir PDFs via sandbox (OpenWebUI serve /sandbox automaticamente)"
        )
        
        auto_detect_environment: bool = Field(
            default=True,
            description="Auto-detectar ambiente (Docker/Local/Standalone)"
        )
        
        pdf_title_default: str = Field(
            default="Relatório", 
            description="Título padrão do PDF"
        )
        
        max_content_chars: int = Field(
            default=500000,
            ge=1000,
            description="Máximo de caracteres permitidos no conteúdo (segurança)"
        )
        
        data_uri_threshold_mb: float = Field(
            default=8.0,
            ge=0.1,
            le=50.0,
            description="Tamanho máximo (MB) para gerar data URI. Acima disso, apenas grava arquivo."
        )
        
        always_generate_data_uri: bool = Field(
            default=True,
            description="Gerar data URI mesmo acima do threshold (pode ser lento para PDFs grandes)"
        )

    def __init__(self):
        self.valves = self.Valves()
        self.citation = False  # Não emite citações custom
        
        # ✅ AUTO-DETECÇÃO de ambiente
        if self.valves.auto_detect_environment:
            # Prioridade 1: OPEN_WEBUI_SANDBOX_PATH (seu ambiente - já testado)
            sandbox_env = os.getenv("OPEN_WEBUI_SANDBOX_PATH")
            if sandbox_env and os.path.exists(sandbox_env):
                self.output_dir = os.path.join(sandbox_env, "pdfs")
            # Prioridade 2: Detectar Docker (data directory)
            elif os.path.exists("/.dockerenv") or os.path.exists("/app/backend"):
                self.output_dir = "/app/backend/data/uploads"
            # Prioridade 3: OpenWebUI local
            elif os.path.exists("./backend/data"):
                self.output_dir = "./backend/data/uploads"
            # Fallback: standalone
            else:
                self.output_dir = "pdf_outputs"
        else:
            self.output_dir = self.valves.pdf_output_dir
        
        # Criar diretório com fallback
        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except PermissionError:
            # Fallback para diretório local
            self.output_dir = os.path.join(os.getcwd(), "pdf_outputs")
            os.makedirs(self.output_dir, exist_ok=True)
    
    def make_pdf_data_uri(self, pdf_bytes: bytes) -> str:
        """
        Gera um data:URI para PDF a partir de bytes.
        
        Args:
            pdf_bytes: Bytes do PDF
            
        Returns:
            String com data URI no formato: data:application/pdf;base64,{b64}
        """
        if not isinstance(pdf_bytes, (bytes, bytearray)) or len(pdf_bytes) == 0:
            raise ValueError("PDF vazio ou inválido")
        
        b64 = base64.b64encode(pdf_bytes).decode("utf-8")
        return f"data:application/pdf;base64,{b64}"

    async def export_pdf(
        self, 
        content: str, 
        filename: Optional[str] = None, 
        title: Optional[str] = None,
        __event_emitter__: Optional[Callable] = None
    ) -> dict:
        """
        Exporta texto para PDF usando ReportLab.
        
        Args:
            content: Conteúdo a exportar (texto ou JSON string)
            filename: Nome do arquivo (opcional, gera timestamp se omitido)
            title: Título do PDF (opcional, usa valve default)
            __event_emitter__: Emitter para status updates (opcional)
        
        Returns:
            Dict com: filename, path, success, [warning/error]
        """
        # Validar tamanho do conteúdo
        if len(content) > self.valves.max_content_chars:
            return {
                "success": False,
                "error": f"Conteúdo muito grande ({len(content)} chars, máximo: {self.valves.max_content_chars})"
            }
        
        # Defaults
        title = title or self.valves.pdf_title_default
        if not filename:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{ts}.pdf"
        
        # Garantir extensão .pdf
        if not filename.endswith('.pdf'):
            filename += '.pdf'
        
        # ✅ GERAR file_id para OpenWebUI (MD5 do filename)
        file_id = hashlib.md5(filename.encode()).hexdigest()[:16]
        dest_filename = f"{file_id}_{filename}"
        filepath = os.path.join(self.output_dir, dest_filename)
        
        # Emitir status inicial
        if __event_emitter__:
            await __event_emitter__({
                "type": "status",
                "data": {"description": f"Gerando PDF: {filename}", "done": False}
            })
        
        # Tentar usar ReportLab
        try:
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas
            from reportlab.lib.units import inch
            
            c = canvas.Canvas(filepath, pagesize=letter)
            width, height = letter
            margin = 0.75 * inch
            y = height - margin
            
            # Título
            c.setFont("Helvetica-Bold", 16)
            c.drawString(margin, y, title)
            y -= 0.3 * inch
            
            # Linha separadora
            c.line(margin, y, width - margin, y)
            y -= 0.2 * inch
            
            # Conteúdo (wrap simples por palavra)
            c.setFont("Helvetica", 10)
            max_width = width - 2 * margin
            line_height = 12
            
            for paragraph in content.split('\n'):
                if not paragraph.strip():
                    y -= line_height  # Linha em branco
                    continue
                
                words = paragraph.split()
                line = ""
                
                for word in words:
                    test_line = f"{line} {word}".strip()
                    
                    # Checar largura
                    if c.stringWidth(test_line, "Helvetica", 10) > max_width:
                        # Linha cheia, desenhar e resetar
                        c.drawString(margin, y, line)
                        y -= line_height
                        line = word
                        
                        # Nova página se necessário
                        if y < margin:
                            c.showPage()
                            y = height - margin
                            c.setFont("Helvetica", 10)
                    else:
                        line = test_line
                
                # Desenhar última linha do parágrafo
                if line:
                    c.drawString(margin, y, line)
                    y -= line_height
                    
                    if y < margin:
                        c.showPage()
                        y = height - margin
                        c.setFont("Helvetica", 10)
            
            c.save()
            
            # ✅ LER BYTES DO PDF GERADO
            with open(filepath, 'rb') as f:
                pdf_bytes = f.read()
            
            size_mb = len(pdf_bytes) / (1024 * 1024)
            
            # ✅ GERAR DATA URI (se dentro do threshold ou forçado)
            data_uri = None
            should_generate_data_uri = (
                self.valves.always_generate_data_uri or 
                size_mb <= self.valves.data_uri_threshold_mb
            )
            
            if should_generate_data_uri:
                try:
                    data_uri = self.make_pdf_data_uri(pdf_bytes)
                except Exception as e:
                    # Falha na geração do data URI não deve interromper
                    data_uri = None
            
            # ✅ GERAR URL PÚBLICA (se base_url configurado)
            public_url = None
            if self.valves.pdf_base_url:
                public_url = f"{self.valves.pdf_base_url}/{file_id}/content"
            
            # ✅ EMITIR LINK CLICÁVEL NO CHAT
            if __event_emitter__:
                # Priorizar data URI (funciona offline), fallback para public_url
                if data_uri:
                    size_info = f" ({size_mb:.1f} MB)" if size_mb > 1.0 else f" ({len(pdf_bytes) / 1024:.0f} KB)"
                    await __event_emitter__({
                        "type": "chat:message",
                        "data": {
                            "role": "assistant",
                            "content": f"📄 **[Baixar {filename}{size_info}]({data_uri})**\n\n_PDF gerado com sucesso! Clique para baixar._",
                            "files": []
                        }
                    })
                elif public_url:
                    await __event_emitter__({
                        "type": "status",
                        "data": {"description": f"PDF disponível em: {public_url}", "done": True}
                    })
                else:
                    await __event_emitter__({
                        "type": "status",
                        "data": {"description": f"PDF salvo: {filepath}", "done": True}
                    })
            
            result = {
                "success": True,
                "filename": filename,
                "path": filepath,
                "file_id": file_id,
                "size_bytes": len(pdf_bytes),
                "size_mb": round(size_mb, 2)
            }
            
            # Adicionar data URI se disponível
            if data_uri:
                result["data_uri"] = data_uri
                result["data_uri_length"] = len(data_uri)
            
            # Adicionar URL HTTP se disponível
            if public_url:
                result["url"] = public_url
            
            return result
        
        except ImportError:
            # Fallback: arquivo de texto com extensão .pdf
            warning = "ReportLab não instalado, gerando arquivo de texto com extensão .pdf"
            
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(f"{title}\n{'=' * len(title)}\n\n{content}")
                
                # ✅ LER BYTES DO ARQUIVO FALLBACK
                with open(filepath, 'rb') as f:
                    pdf_bytes = f.read()
                
                size_mb = len(pdf_bytes) / (1024 * 1024)
                
                # ✅ GERAR DATA URI PARA FALLBACK TAMBÉM
                data_uri = None
                should_generate_data_uri = (
                    self.valves.always_generate_data_uri or 
                    size_mb <= self.valves.data_uri_threshold_mb
                )
                
                if should_generate_data_uri:
                    try:
                        # Para fallback .txt, usar text/plain
                        b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                        data_uri = f"data:text/plain;base64,{b64}"
                    except Exception:
                        data_uri = None
                
                # Gerar URL HTTP se disponível
                public_url = None
                if self.valves.pdf_base_url:
                    public_url = f"{self.valves.pdf_base_url}/{file_id}/content"
                
                # ✅ EMITIR LINK CLICÁVEL NO CHAT
                if __event_emitter__:
                    if data_uri:
                        size_info = f" ({size_mb:.1f} MB)" if size_mb > 1.0 else f" ({len(pdf_bytes) / 1024:.0f} KB)"
                        await __event_emitter__({
                            "type": "chat:message",
                            "data": {
                                "role": "assistant",
                                "content": f"⚠️ **[Baixar {filename}{size_info}]({data_uri})**\n\n_Fallback: ReportLab não disponível. Arquivo gerado como texto simples._",
                                "files": []
                            }
                        })
                    elif public_url:
                        await __event_emitter__({
                            "type": "status",
                            "data": {"description": f"Fallback PDF em: {public_url}", "done": True}
                        })
                    else:
                        await __event_emitter__({
                            "type": "status",
                            "data": {"description": f"Fallback: {filepath}", "done": True}
                        })
                
                result = {
                    "success": True,
                    "filename": filename,
                    "path": filepath,
                    "file_id": file_id,
                    "warning": warning,
                    "size_bytes": len(pdf_bytes),
                    "size_mb": round(size_mb, 2)
                }
                
                if data_uri:
                    result["data_uri"] = data_uri
                    result["data_uri_length"] = len(data_uri)
                
                if public_url:
                    result["url"] = public_url
                
                return result
            
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Falha no fallback: {str(e)}"
                }
        
        except Exception as e:
            # Erro no ReportLab
            if __event_emitter__:
                await __event_emitter__({
                    "type": "status",
                    "data": {"description": f"Erro: {str(e)}", "done": True}
                })
            
            return {
                "success": False,
                "error": f"Erro ao gerar PDF: {str(e)}"
            }
