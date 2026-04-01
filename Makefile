REPORT_SRC    := docs/reports/interim/promise-sentry-interim-report.md
COVER         := docs/reports/cover.md
STYLESHEET    := docs/reports/pdf-style.css

REPORT_OUT    := docs/reports/interim/promise-sentry-interim-report.pdf

TMP_RENDERED  := /tmp/vs-report-rendered.md
TMP_INLINED   := /tmp/vs-report-inlined.md
TMP_COMBINED  := /tmp/vs-report-combined.md

.PHONY: pdf clean-pdf pdf-file


## pdf: render Mermaid diagrams and convert report to PDF
pdf: $(REPORT_OUT)

$(REPORT_OUT): $(REPORT_SRC) $(COVER) $(STYLESHEET)
	@echo "→ Pre-rendering Mermaid diagrams..."
	bunx @mermaid-js/mermaid-cli -i $(REPORT_SRC) -o $(TMP_RENDERED) --outputFormat png --scale 3 --quiet
	@echo "→ Inlining diagrams as base64..."
	sed -i 's|](./|](/tmp/|g' $(TMP_RENDERED)
	python3 docs/reports/inline-images.py $(TMP_RENDERED) > $(TMP_INLINED)
	@echo "→ Prepending cover page..."
	cat $(COVER) $(TMP_INLINED) > $(TMP_COMBINED)
	@echo "→ Converting to PDF..."
	bunx md-to-pdf \
		--stylesheet $(abspath $(STYLESHEET)) \
		--pdf-options '{"format":"A4","margin":{"top":"25mm","bottom":"25mm","left":"22mm","right":"22mm"},"printBackground":true}' \
		$(TMP_COMBINED)
	mv /tmp/vs-report-combined.pdf $(REPORT_OUT)
	@echo "✓ PDF written to $(REPORT_OUT)"

## pdf-file input=<path>: convert a single markdown file to PDF in the same directory
pdf-file:
ifndef input
	$(error input is required: make pdf-file input=/path/to/file.md)
endif
	@echo "→ Pre-rendering Mermaid diagrams..."
	bunx @mermaid-js/mermaid-cli -i $(input) -o /tmp/vs-pdffile-rendered.md --outputFormat png --scale 3 --quiet
	@echo "→ Fixing image paths..."
	sed -i 's|](./|](/tmp/|g' /tmp/vs-pdffile-rendered.md
	@echo "→ Inlining diagrams as base64..."
	python3 docs/reports/inline-images.py /tmp/vs-pdffile-rendered.md > /tmp/vs-pdffile-inlined.md
	@echo "→ Converting to PDF..."
	bunx md-to-pdf \
		--stylesheet $(abspath $(STYLESHEET)) \
		--pdf-options '{"format":"A4","margin":{"top":"25mm","bottom":"25mm","left":"22mm","right":"22mm"},"printBackground":true}' \
		/tmp/vs-pdffile-inlined.md
	mv /tmp/vs-pdffile-inlined.pdf $(patsubst %.md,%.pdf,$(input))
	rm -f /tmp/vs-pdffile-rendered.md /tmp/vs-pdffile-inlined.md
	@echo "✓ PDF written to $(patsubst %.md,%.pdf,$(input))"

## clean-pdf: remove generated PDF and temp files
clean-pdf:
	rm -f $(REPORT_OUT) $(TMP_RENDERED) $(TMP_INLINED) $(TMP_COMBINED)
	@echo "✓ Cleaned PDF artifacts"
