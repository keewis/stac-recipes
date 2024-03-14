# STAC recipes design

## `pystac`: attaching items to a catalog / collection

The `add_item` method on `Catalog` does the following things (`add_items` delegates):

1. check the new item is not a catalog
2. verify the strategy
3. set the root of the item
4. decide whether to set the parent:
   - if `set_parent` is True (the default), set the catalog as the item's parent
   - otherwise set the item's `_allow_parent_to_overrride_href` to False
5. self link:
   - skip if `set_parent` is false or the catalog does not have a self href
   - otherwise use the strategy to get a item href and set it as the self link
6. create a link to the item and attach it to the catalog, then return it

## `pystac`: attaching catalogs to other catalogs

The `add_child` method on `Catalog` does the exact same things as `add_item`, except it rejects items and has different variable names (`add_items` delegates).

## `pystac`: saving catalogs

The `save` method on `pystac.Catalog` does the following things:

1. check that there's a root catalog
2. overwrite the catalog type if given
3. determine if a self link should be included for items
4. iterate over child catalog links:
   1. skip all unresolved links
   2. cast the link target to a catalog
   3. if `dest_href` was given:
      - determine the path of the child relative to the current catalog
      - construct an absolute path from the relative path and `dest_href`
      - pass the directory name of the absolute path as `dest_href` to the child's `save`
   4. otherwise call the child's `save` without `dest_href`
5. iterate over item links
   1. skip all unresolved links
   2. cast the link target to a item
   3. if `dest_href` was given
      - determine the path of the item relative to the current catalog
      - construct an absolute path from the relative path and `dest_href`
      - pass the directory name of the absolute path as `dest_href` to the item's `save_object` and the flag determining self links
   4. otherwise call `save_object` without `dest_href`
6. determine if the catalog should have self links
   - if an absolute catalog: yes
   - if not a self-contained catalog and the current catalog is the root: yes
   - otherwise no
7. determine the catalog's `dest_href`:
   - if `dest_href` is not None, compute the relative path of self to self and combine it with `dest_href`
   - otherwise use `None`
8. call `save_object` with `dest_href` and the flag to determine self links

So it looks like the write happens recursively, and we need to figure out how to extract the `save_object` calls and transform this into an iterator.
