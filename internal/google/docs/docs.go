// Package docs is the Docs v1 JSON the vfs adapter reads.
package docs

// Document is documents.get with includeTabsContent: the tabs, or (a
// document read without tabs) the one body.
type Document struct {
	Title         string                  `json:"title,omitempty"`
	Body          *Body                   `json:"body,omitempty"`
	Lists         map[string]List         `json:"lists,omitempty"`
	InlineObjects map[string]InlineObject `json:"inlineObjects,omitempty"`
	Tabs          []*Tab                  `json:"tabs,omitempty"`
}

// Tab is one document tab and its child tabs.
type Tab struct {
	TabProperties *TabProperties `json:"tabProperties,omitempty"`
	DocumentTab   *DocumentTab   `json:"documentTab,omitempty"`
	ChildTabs     []*Tab         `json:"childTabs,omitempty"`
}

// TabProperties names a tab.
type TabProperties struct {
	TabID string `json:"tabId,omitempty"`
	Title string `json:"title,omitempty"`
	Index int64  `json:"index,omitempty"`
}

// DocumentTab is a tab's content.
type DocumentTab struct {
	Body          *Body                   `json:"body,omitempty"`
	Lists         map[string]List         `json:"lists,omitempty"`
	InlineObjects map[string]InlineObject `json:"inlineObjects,omitempty"`
}

// Body is a tab's structural elements.
type Body struct {
	Content []*StructuralElement `json:"content,omitempty"`
}

// StructuralElement is one paragraph or table.
type StructuralElement struct {
	Paragraph *Paragraph `json:"paragraph,omitempty"`
	Table     *Table     `json:"table,omitempty"`
}

// Table is a table's rows.
type Table struct {
	TableRows []*TableRow `json:"tableRows,omitempty"`
}

// TableRow is a row's cells.
type TableRow struct {
	TableCells []*TableCell `json:"tableCells,omitempty"`
}

// TableCell is a cell's content.
type TableCell struct {
	Content []*StructuralElement `json:"content,omitempty"`
}

// Paragraph is a paragraph's runs and style.
type Paragraph struct {
	Elements       []*ParagraphElement `json:"elements,omitempty"`
	ParagraphStyle *ParagraphStyle     `json:"paragraphStyle,omitempty"`
	Bullet         *Bullet             `json:"bullet,omitempty"`
}

// ParagraphElement is a text run or an inline object.
type ParagraphElement struct {
	TextRun             *TextRun             `json:"textRun,omitempty"`
	InlineObjectElement *InlineObjectElement `json:"inlineObjectElement,omitempty"`
}

// TextRun is styled text.
type TextRun struct {
	Content   string     `json:"content,omitempty"`
	TextStyle *TextStyle `json:"textStyle,omitempty"`
}

// TextStyle is the run style the adapter reads.
type TextStyle struct {
	Bold          bool  `json:"bold,omitempty"`
	Italic        bool  `json:"italic,omitempty"`
	Strikethrough bool  `json:"strikethrough,omitempty"`
	Link          *Link `json:"link,omitempty"`
}

// Link is a hyperlink target.
type Link struct {
	URL string `json:"url,omitempty"`
}

// ParagraphStyle is a paragraph's named style.
type ParagraphStyle struct {
	NamedStyleType string `json:"namedStyleType,omitempty"`
}

// Bullet puts a paragraph in a list.
type Bullet struct {
	ListID       string `json:"listId,omitempty"`
	NestingLevel int64  `json:"nestingLevel,omitempty"`
}

// List is a list's properties.
type List struct {
	ListProperties *ListProperties `json:"listProperties,omitempty"`
}

// ListProperties is a list's levels.
type ListProperties struct {
	NestingLevels []*NestingLevel `json:"nestingLevels,omitempty"`
}

// NestingLevel is one list level.
type NestingLevel struct {
	GlyphType string `json:"glyphType,omitempty"`
}

// InlineObject is an object a paragraph embeds.
type InlineObject struct {
	InlineObjectProperties *InlineObjectProperties `json:"inlineObjectProperties,omitempty"`
}

// InlineObjectProperties holds the embedded object.
type InlineObjectProperties struct {
	EmbeddedObject *EmbeddedObject `json:"embeddedObject,omitempty"`
}

// EmbeddedObject is an embedded image.
type EmbeddedObject struct {
	Title           string           `json:"title,omitempty"`
	ImageProperties *ImageProperties `json:"imageProperties,omitempty"`
}

// ImageProperties locates an image.
type ImageProperties struct {
	ContentURI string `json:"contentUri,omitempty"`
}

// InlineObjectElement places an inline object.
type InlineObjectElement struct {
	InlineObjectID string `json:"inlineObjectId,omitempty"`
}
