package usecase

import (
	"image"
	"image/color"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBuildStickerWebPFFmpegArgs(t *testing.T) {
	want := []string{
		"-y",
		"-i", "input.png",
		"-vcodec", "libwebp",
		"-lossless", "0",
		"-compression_level", "6",
		"-q:v", "60",
		"-preset", "default",
		"-loop", "0",
		"-an",
		"output.webp",
	}

	got := buildStickerWebPFFmpegArgs("input.png", "output.webp")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %v, want %v", got, want)
	}
	for _, arg := range got {
		if arg == "-vsync" {
			t.Fatal("ffmpeg 9 removed -vsync; passing it fails the whole conversion")
		}
	}
}

func TestBuildStickerWebPFFmpegArgsConvertWithInstalledFFmpeg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	if encoders, err := exec.Command("ffmpeg", "-hide_banner", "-encoders").Output(); err != nil || !strings.Contains(string(encoders), "libwebp") {
		t.Skip("ffmpeg built without libwebp")
	}

	dir := t.TempDir()
	pngPath := filepath.Join(dir, "sticker.png")
	webpPath := filepath.Join(dir, "sticker.webp")

	img := image.NewNRGBA(image.Rect(0, 0, 16, 16))
	for x := 0; x < 16; x++ {
		img.Set(x, x, color.NRGBA{R: 255, A: 255})
	}
	f, err := os.Create(pngPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := png.Encode(f, img); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command("ffmpeg", buildStickerWebPFFmpegArgs(pngPath, webpPath)...).CombinedOutput()
	if err != nil {
		t.Fatalf("ffmpeg failed: %v\n%s", err, out)
	}
	if info, err := os.Stat(webpPath); err != nil || info.Size() == 0 {
		t.Fatalf("expected non-empty webp output, stat err=%v", err)
	}
}
